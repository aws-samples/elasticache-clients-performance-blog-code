<?php
require "vendor/autoload.php";
require "vendor/predis/predis/autoload.php";
Predis\Autoloader::register();

$predis;
$phpredis;
const SIZE_SET_KEYSPACE = 3000000; // 3 million
const SIZE_GET_KEYSPACE = 3750000; // 3.75 million
const PROB_GET = 0.8;

function generateKeyGet()
{
    return rand(1, SIZE_GET_KEYSPACE);
}

function generateKeySet()
{
    return rand(1, SIZE_SET_KEYSPACE);
}

function generatePayloadSize()
{
    return max(2, nrand());
}

function nrand()
{
    $x = mt_rand() / mt_getrandmax();
    $y = mt_rand() / mt_getrandmax();
    return (int) (sqrt(-2 * log($x)) * cos(2 * pi() * $y) * 400 + 1024);
}

function shouldGet()
{
    return mt_rand() / mt_getrandmax() < PROB_GET;
}

function initTest($host, $port) {
    global $predis;
    global $phpredis;
    $predis = new Predis\Client([
        "host" => $host,
        "port" => $port,
    ]);
    $phpredis = new Redis();
    $phpredis->connect($host, $port);
    mt_srand(0);
    srand(0);
}

function getPipe($client)
{
    global $predis;
    global $phpredis;
    if ($client == "predis") {
        return $predis->pipeline();
    }
    return $phpredis->multi(Redis::PIPELINE);
}

function execPipe($client, $pipe)
{
    if ($client == "predis") {
        $pipe->execute();
        return;
    }
    $pipe->exec();
}

function warmup()
{
    global $phpredis;
    printf("starting warmup\n");
    $start = microtime(true);
    $phpredis->flushAll();
    $numPipelines = SIZE_SET_KEYSPACE / 1000;
    for ($i = 0; $i < $numPipelines; $i++) {
        $pipe = $phpredis->pipeline();
        for ($j = 1; $j <= 1000; $j++) {
            $pipe->set(
                1000 * $i + $j,
                str_pad("0", generatePayloadSize(), "0")
            );
        }
        $pipe->exec();
    }
    $duration = microtime(true) - $start;
    printf("completed warmup. duration: %.2f\n", $duration);
}

function pipelineTest($totalCommands, $pipelineSize, $client)
{
    assert(
        $totalCommands % $pipelineSize == 0,
        "Error totalCommands not divisible by pipelineSize\n"
    );
    warmup();
    printf("starting predis pipeline test. Pipeline size: %d\n", $pipelineSize);
    $numPipelines = $totalCommands / $pipelineSize;
    $start = microtime(true);
    for ($i = 0; $i < $numPipelines; $i++) {
        $pipe = getPipe($client);
        for ($j = 0; $j < $pipelineSize; $j++) {
            if (shouldGet()) {
                $pipe->get(generateKeyGet());
            } else {
                $pipe->set(
                    generateKeySet(),
                    str_pad("0", generatePayloadSize(), "0")
                );
            }
        }
        execPipe($client, $pipe);
    }
    $duration = microtime(true) - $start;
    printf(
        "completed %s Pipeline Test. Pipeline size: %d. TPS: %.2f\n",
        $client,
        $pipelineSize,
        $totalCommands / $duration
    );
}

initTest("localhost", 6379);

// phpredis pipeline test
pipelineTest(3000000, 1, "phpredis");
pipelineTest(3000000, 2, "phpredis");
pipelineTest(3000000, 3, "phpredis");
pipelineTest(3000000, 10, "phpredis");
pipelineTest(3000000, 20, "phpredis");
pipelineTest(3000000, 50, "phpredis");
pipelineTest(10000000, 100, "phpredis");
pipelineTest(10000000, 1000, "phpredis");

// predis pipeline test
pipelineTest(3000000, 1, "predis");
pipelineTest(3000000, 2, "predis");
pipelineTest(3000000, 3, "predis");
pipelineTest(3000000, 10, "predis");
pipelineTest(3000000, 20, "predis");
pipelineTest(3000000, 50, "predis");
pipelineTest(10000000, 100, "predis");
pipelineTest(10000000, 1000, "predis");
?>
