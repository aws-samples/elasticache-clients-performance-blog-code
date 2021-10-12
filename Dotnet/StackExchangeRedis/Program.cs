using System;
using StackExchange.Redis;
using System.Threading.Tasks;
using System.Threading;
using System.Collections.Generic;
using System.Diagnostics;

namespace StackExchangeRedis
{
    static class Program
    {
        private const string Host = "localhost";
        private const double ProbGet = 0.8;
        private const int SizeGetKeySpace = 3750000; // 3.75 million
        private const int SizeSetKeySpace = 3000000; // 3 million

        private static string GenerateKeySet()
        {
            return rndObj.Next(1, SizeSetKeySpace + 1).ToString();
        }


        private static string GenerateKeyGet()
        {
            return rndObj.Next(1, SizeGetKeySpace + 1).ToString();
        }


        private static int GeneratePayloadSize()
        {
            var payload = SampleGaussian();
            var posPayload = Math.Max(payload, 2);
            return posPayload;
        }


        private static Boolean ShouldGet()
        {
            return rndObj.NextDouble() < ProbGet;
        }


        private static void Warmup()
        {
            Console.WriteLine("starting warmup");
            Stopwatch timer = new Stopwatch();
            timer.Restart();
            server.FlushDatabase();
            var numPipelines = SizeSetKeySpace / 1000;
            for (var i = 0; i < numPipelines; i++)
            {
                IBatch batch = db.CreateBatch();
                var tasks = new Task[1000];
                for (var j = 1; j <= 1000; j++)
                {
                    tasks[j-1] = batch.StringSetAsync((i * 1000 + j).ToString(), new string('0', GeneratePayloadSize()));
                }
                batch.Execute();
                db.WaitAll(tasks);
            }
            timer.Stop();
            Console.WriteLine($"Completed warmup, duration {timer.Elapsed.Seconds} seconds");
        }


        private static void SyncWorkerThread(long numCommands)
        {
            for (var i = 0; i < numCommands; i++)
            {
                if (ShouldGet())
                {
                    db.StringGet(GenerateKeyGet());
                }
                else
                {
                    db.StringSet(GenerateKeySet(), new string('0', GeneratePayloadSize()));
                }
            }
        }


        private static void AsyncWorkerThread(long numCommands)
        {
            for (var i = 0; i < numCommands; i++)
            {
                Task t;
                if (ShouldGet())
                {
                    t = db.StringGetAsync(GenerateKeyGet());
                }
                else
                {
                    t = db.StringSetAsync(GenerateKeySet(), new string('0', GeneratePayloadSize()));
                }
                db.Wait(t);
            }
        }


        private static void MultiplexorTest(int numThreads, long totalCommands, Action<long> worker)
        {
            Debug.Assert(totalCommands % numThreads == 0, "Error totalCommands not divisible by numThreads");
            Warmup();
            string api = worker.Method.Name == "SyncWorkerThread" ? "Sync" : "Async";
            Console.WriteLine($"Starting {api} Multiplexor Test, Num Threads = {numThreads}");
            long commandsPerThread = totalCommands / numThreads;
            Stopwatch timer = new Stopwatch();
            timer.Restart();
            var threads = new List<Thread>();
            for (var i = 0; i < numThreads; i++)
            {
                var t = new Thread(() => worker(commandsPerThread));
                t.Start();
                threads.Add(t);
            }
            foreach (Thread t in threads)
            {
                t.Join();
            }
            timer.Stop();
            Console.WriteLine($"Completed Test, TPS: {(totalCommands * 1000) / timer.ElapsedMilliseconds}");
        }


        private static void PipelineTest(int pipelineSize, long totalCommands)
        {
            Debug.Assert(totalCommands % pipelineSize == 0, "Error totalCommands not divisible by pipelineSize");
            Warmup();
            Console.WriteLine($"Starting Pipeline Test, Pipeline Size = {pipelineSize}");
            Stopwatch timer = new Stopwatch();
            var numPipelines = totalCommands / pipelineSize;
            timer.Restart();
            for (var i = 0; i < numPipelines; i++)
            {
                var tasks = new Task[pipelineSize];
                for (var j = 0; j < pipelineSize; j++)
                {
                    if (ShouldGet())
                    {
                        tasks[j] = db.StringGetAsync(GenerateKeyGet());
                    }
                    else
                    {
                        tasks[j] = db.StringSetAsync(GenerateKeySet(), new string('0', GeneratePayloadSize()));
                    }
                }
                db.WaitAll(tasks);
            }
            timer.Stop();
            Console.WriteLine($"Completed Test, TPS: {(totalCommands*1000)/timer.ElapsedMilliseconds}");
        }


        private static void BatchTest(int batchSize, long totalCommands)
        {
            Debug.Assert(totalCommands % batchSize == 0, "Error totalCommands not divisible by batchSize");
            Warmup();
            Console.WriteLine($"Starting Batch Test, Batch Size = {batchSize}");
            Stopwatch timer = new Stopwatch();
            var numBatches = totalCommands / batchSize;
            timer.Restart();
            for (var i = 0; i < numBatches; i++)
            {
                IBatch batch = db.CreateBatch();
                var tasks = new Task[batchSize];
                for (var j = 0; j < batchSize; j++)
                {
                    if (ShouldGet())
                    {
                        tasks[j] = batch.StringGetAsync(GenerateKeyGet());
                    }
                    else
                    {
                        tasks[j] = batch.StringSetAsync(GenerateKeySet(), new string('0', GeneratePayloadSize()));
                    }
                }
                batch.Execute();
                db.WaitAll(tasks);
            }
            timer.Stop();
            Console.WriteLine($"Completed Test, TPS: {(totalCommands * 1000) / timer.ElapsedMilliseconds}");
        }


        private static int SampleGaussian()
        {
            // The method requires sampling from a uniform random of (0,1]
            // but Random.NextDouble() returns a sample of [0,1).
            double x1 = 1 - rndObj.NextDouble();
            double x2 = 1 - rndObj.NextDouble();

            double y1 = Math.Sqrt(-2.0 * Math.Log(x1)) * Math.Cos(2.0 * Math.PI * x2);
            return (int)(y1 * 400) + 1024;
        }


        private static ConnectionMultiplexer cm = ConnectionMultiplexer.Connect($"{Host}, allowAdmin=true, syncTimeout=30000");
        private static IServer server = cm.GetServer(Host, 6379);
        private static IDatabase db = cm.GetDatabase();
        private static System.Random rndObj = new System.Random();
    
        static public void Main(string[] args)
        {
            // sync multiplexor tests
            MultiplexorTest(1, 30000, SyncWorkerThread);
            MultiplexorTest(2, 30000, SyncWorkerThread);
            MultiplexorTest(10, 120000, SyncWorkerThread);
            MultiplexorTest(20, 120000, SyncWorkerThread);
            MultiplexorTest(30, 120000, SyncWorkerThread);
            MultiplexorTest(40, 120000, SyncWorkerThread);
            MultiplexorTest(50, 120000, SyncWorkerThread);
            MultiplexorTest(60, 120000, SyncWorkerThread);

            // async multiplexor tests
            MultiplexorTest(1, 3000000, AsyncWorkerThread);
            MultiplexorTest(2, 3000000, AsyncWorkerThread);
            MultiplexorTest(10, 15000000, AsyncWorkerThread);
            MultiplexorTest(20, 15000000, AsyncWorkerThread);
            MultiplexorTest(30, 15000000, AsyncWorkerThread);
            MultiplexorTest(40, 25000000, AsyncWorkerThread);
            MultiplexorTest(50, 30000000, AsyncWorkerThread);
            MultiplexorTest(60, 3000000, AsyncWorkerThread);

            // pipeline tests
            PipelineTest(3, 3000000);
            PipelineTest(10, 10000000);
            PipelineTest(100, 40000000);
            PipelineTest(1000, 60000000);

            // batching tests
            BatchTest(3, 30000);
            BatchTest(10, 100000);
            BatchTest(100, 400000);
            BatchTest(1000, 600000);
        }
    }
}
