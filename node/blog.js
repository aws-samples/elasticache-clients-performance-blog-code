const assert = require('assert');
const redis = require("redis");
const uniform = require("uniform-integer");
const gaussian = require("gaussian");
const guassianDistribution = gaussian(1024, 400 * 400); // mean = 1024, variance = 400*400

const HOST = "localhost";
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
        const batch = clients[0].batch();
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
    clients[0].flushall(() => {
        const batch = clients[0].batch();
        for (let i = 0; i < 1000; i++) {
            batch.set(i, "0".repeat(generatePayloadSize()), () => cbWarmup(start));
        }
        batch.exec();
    })
}

function cbBoundedConcurrency(bound, numClients, totalCommands, start) {
    completedCommands++;
    if (completedCommands >= totalCommands) {
        const duration = (Date.now() - start) / 1000;
        console.log(`completed bounded concurrency test. Bound: ${bound}, numClients: ${numClients}, TPS: ${(totalCommands)/duration}`);
        setImmediate(next);
        return;
    }
    if (sentCommands < totalCommands) {
        sentCommands++;
        const client = clients[completedCommands % numClients];
        if (shouldGet()) {
            client.get(generateKeyGet(), () => cbBoundedConcurrency(bound, numClients, totalCommands, start));
        } else {
            client.set(generateKeySet(), "0".repeat(generatePayloadSize()), () => cbBoundedConcurrency(bound, numClients, totalCommands, start));
        }
    }
}

function boundedConcurrencyTest(bound, numClients, totalCommands) {
    assert(totalCommands >= bound);
    console.log(`starting bounded concurrency test. bound: ${bound}, num clients ${numClients}`);
    completedCommands = 0;
    sentCommands = 0;
    const start = Date.now();
    for (let i = 0; i < bound; i++) {
        const client = clients[i % numClients]
        if (shouldGet()) {
            client.get(generateKeyGet(), () =>
                cbBoundedConcurrency(bound, numClients, totalCommands, start));
        } else {
            client.set(generateKeySet(), "0".repeat(generatePayloadSize()), () =>
                cbBoundedConcurrency(bound, numClients, totalCommands, start));
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
        const batch = clients[0].batch();
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
    const batch = clients[0].batch();
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
    } else {
        clients.forEach((client) => {
            client.quit()
        });
    }
}

let tests = [];
let clients = [];
let readyClients = 0;
let completedCommands = 0;
let sentCommands = 0;

// Bounded Concurrency Tests 1 Client
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1, 1, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(2, 1, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(10, 1, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(100, 1, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1000, 1, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(10000, 1, 3000000));

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

// Multi Client Round Robin Tests
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1000, 2, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1000, 3, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1000, 10, 3000000));
tests.push(warmup);
tests.push(() => boundedConcurrencyTest(1000, 100, 3000000));

// Initialize Clients and Run Script
for (let i = 0; i < 100; i++) {
    const client = redis.createClient(URL);
    clients.push(client);
    client.on("ready", () => {
        readyClients++;
        if (readyClients == 100) {
            setImmediate(next);
        }
    })
}