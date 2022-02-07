const assert = require('assert');
const redis = require("redis");
const uniform = require("uniform-integer");
const gaussian = require("gaussian");
const guassianDistribution = gaussian(1024, 400 * 400); // mean = 1024, variance = 400*400

const HOST = "blog.ftsojn.0001.usw2.cache.amazonaws.com";
const URL = "redis://" + HOST;
const PROB_GET = 0.8;
SIZE_GET_KEY_SPACE = 375000000; // 3.75 million
SIZE_SET_KEY_SPACE = 3000000; // 3 million

function generateKeyGet() {
    // between 1 and SIZE_GET_KEY_SPACE - 1 inclusive
    return uniform(0, SIZE_GET_KEY_SPACE - 1);
}

function generateKeySet() {
    // between 1 and SIZE_SET_KEY_SPACE - 1 inclusive
    return uniform(0, SIZE_SET_KEY_SPACE - 1);
}

function generatePayloadSize() {
    let payload = Math.floor(guassianDistribution.ppf(Math.random()));
    let posPayload = Math.max(2, payload);
    return posPayload;
}

function shouldGet() {
    return Math.random() < PROB_GET;
}

function cbWarmup(start) {
    completedCommands++;
    if (completedCommands == SIZE_SET_KEY_SPACE) {
        const duration = (Date.now() - start) / 1000;
        console.log(`warmp up completed ${duration} seconds`);
        setImmediate(next);
        return;
    }
    if (completedCommands % 1000 == 0) {
        const batch = client.batch();
        for (let i = 0; i < 1000; i++) {
            batch.set(completedCommands + i, "0".repeat(generatePayloadSize()), () => cbWarmup(start));
        }
        batch.exec();
    }
}

function warmup() {
    console.log("starting warmup");
    completedCommands = 0;
    const start = Date.now();
    client.flushall(() => {
        const batch = client.batch();
        for (let i = 0; i < 1000; i++) {
            batch.set(i, "0".repeat(generatePayloadSize()), () => cbWarmup(start));
        }
        batch.exec();
    })
}

function cbBoundedConcurrency(bound, totalCommands, start) {
    completedCommands++;
    if (completedCommands >= totalCommands) {
        const duration = (Date.now() - start) / 1000;
        console.log(`completed bounded concurrency test. bound: ${bound}, TPS: ${(totalCommands)/duration}`);
        setImmediate(next);
        return;
    }
    if (sentCommands < totalCommands) {
        sentCommands++;
        if (shouldGet()) {
            client.get(generateKeyGet(), () => cbBoundedConcurrency(bound, totalCommands, start));
        } else {
            client.set(generateKeySet(), "0".repeat(generatePayloadSize()), () => cbBoundedConcurrency(bound, totalCommands, start));
        }
    }
}

function boundedConcurrencyTest(bound, totalCommands) {
    assert(totalCommands >= bound);
    console.log(`starting bounded concurrency test. bound: ${bound}`);
    completedCommands = 0;
    sentCommands = 0;
    const start = Date.now();
    for (let i = 0; i < bound; i++) {
        if (shouldGet()) {
            client.get(generateKeyGet(), () =>
                cbBoundedConcurrency(bound, totalCommands, start));
        } else {
            client.set(generateKeySet(), "0".repeat(generatePayloadSize()), () =>
                cbBoundedConcurrency(bound, totalCommands, start));
        }
    }
    sentCommands = bound;
}

function cbBatch(start, batchSize, totalCommands) {
    completedCommands++;
    if (completedCommands == totalCommands) {
        const duration = (Date.now() - start) / 1000;
        console.log(`completed batch test. batch size: ${batchSize}, TPS: ${(totalCommands)/duration}`);
        setImmediate(next);
        return;
    }
    if (completedCommands % batchSize == 0) {
        const batch = client.batch();
        for (let i = 0; i < batchSize; i++) {
            if (shouldGet()) {
                batch.get(generateKeyGet(), () => cbBatch(start, batchSize, totalCommands));
            } else {
                batch.set(generateKeySet(), "0".repeat(generatePayloadSize()), () => cbBatch(start, batchSize, totalCommands));
            }
        }
        batch.exec();
    }
}

function batchTest(batchSize, totalCommands) {
    assert(totalCommands % batchSize == 0, "Error totalCommands not divisible by batchSize");
    console.log(`starting batch test. Batch Size: ${batchSize}`);
    completedCommands = 0;
    const start = Date.now();
    const batch = client.batch();
    for (let i = 0; i < batchSize; i++) {
        if (shouldGet()) {
            batch.get(generateKeyGet(), () => cbBatch(start, batchSize, totalCommands));
        } else {
            batch.set(generateKeySet(), "0".repeat(generatePayloadSize()), () => cbBatch(start, batchSize, totalCommands));
        }
    }
    batch.exec();
}

function next() {
    var test = tests.shift();
    if (test) {
        test();
    } else{
        client.quit();
    }
}

let tests = [];
let client = redis.createClient(URL);
let completedCommands = 0;
let sentCommands = 0;

// Bounded Concurrency Tests
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(2, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(10, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(100, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1000, 3000000));

// Batch Tests
tests.push(warmup);
tests.push(() => batchTest(2, 3000000));
tests.push(warmup);
tests.push(() => batchTest(3, 3000000));
tests.push(warmup);
tests.push(() => batchTest(10, 3000000));
tests.push(warmup);
tests.push(() => batchTest(100, 9000000));
tests.push(warmup);
tests.push(() => batchTest(1000, 9000000));

setImmediate(next);
