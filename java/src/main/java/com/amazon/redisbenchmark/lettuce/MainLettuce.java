package com.amazon.redisbenchmark.lettuce;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class MainLettuce {

  public static final String HOST =
    "localhost";
  public static final int PORT = 6379;
  public static final int NUM_GET_KEYS = 3750000;
  public static final int NUM_SET_KEYS = (int) (NUM_GET_KEYS * 0.8);
  public static final int NORMAL_MEAN = 1024;
  public static final int NORMAL_STD = 400;
  public static final double PROBABILITY_OF_GET = 0.8;
  public static int PIPELINE_SIZE = 100; // This params might change during tests
  public static int NUM_OF_THREADS = 10; // This params might change during tests
  public static int COMMANDS_PER_TEST = 1000 * 1000 * 10; // This params might change during tests
  public static Random r;

  static {
    r = new Random();
  }

  public MainLettuce() {}

  private static Integer getSetKey(Random rand) {
    return rand.nextInt(NUM_SET_KEYS) + 1;
  }

  private static Integer getGetKey(Random rand) {
    return rand.nextInt(NUM_GET_KEYS) + 1;
  }

  private static int getValueSize(Random rand) {
    return (int) Math.round(rand.nextGaussian() * NORMAL_STD + NORMAL_MEAN);
  }

  private static String getValueString(int valueSize) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < valueSize; i++) {
      stringBuilder.append("0");
    }
    return stringBuilder.toString();
  }

  // False for GET
  // True for SET
  private static Boolean shouldSet(Random rand) {
    return (PROBABILITY_OF_GET < rand.nextFloat());
  }

  public static void runSyncQueryToServer(
    Random rand,
    RedisCommands<String, String> syncCommands
  ) {
    if (shouldSet(rand)) { // True for SET
      syncCommands.set(
        getSetKey(rand).toString(),
        getValueString(getValueSize(rand))
      );
    } else { // False for GET
      syncCommands.get(getGetKey(rand).toString());
    }
  }

  public static void addAsyncQueryToConnection(
    Random rand,
    List<RedisFuture<String>> futures,
    RedisAsyncCommands<String, String> asyncCommands
  ) {
    if (shouldSet(rand)) { // True for SET
      futures.add(
        asyncCommands.set(
          getSetKey(rand).toString(),
          getValueString(getValueSize(rand))
        )
      );
    } else { // False for GET
      futures.add(asyncCommands.get(getGetKey(rand).toString()));
    }
  }

  private static void warmup() {
    RedisClient redis = null;
    StatefulRedisConnection<String, String> connection = null;
    try {
      RedisURI redisUri = RedisURI.Builder.redis(HOST).withDatabase(0).build();
      redis = RedisClient.create(redisUri);
      connection = redis.connect();
      RedisCommands<String, String> syncCommands = connection.sync();
      syncCommands.flushall();

      RedisAsyncCommands<String, String> asyncCommands = connection.async();
      final int warmupPipelineSize = 1000;
      for (int i = 0; i < NUM_SET_KEYS / warmupPipelineSize; ++i) {
        List<RedisFuture<String>> futures = new ArrayList<RedisFuture<String>>();
        for (int j = 0; j < warmupPipelineSize; ++j) {
          futures.add(
            asyncCommands.set(
              Integer.valueOf(i * warmupPipelineSize + j).toString(),
              getValueString(getValueSize(r))
            )
          );
        }
        LettuceFutures.awaitAll(
          5,
          TimeUnit.SECONDS,
          futures.toArray(new RedisFuture[futures.size()])
        );
      }
    } finally {
      if (connection != null) {
        connection.close();
      }

      System.out.println("Warmup done");

      if (redis != null) {
        redis.shutdown();
      }
    }
  }

  public static void showPoolInfo(
    GenericObjectPool<StatefulRedisConnection<String, String>> pool
  ) {
    System.out.println("pool.getNumWaiters: " + pool.getNumWaiters());
    System.out.println("pool.getNumActive: " + pool.getNumActive());
    System.out.println("pool.getNumIdle: " + pool.getNumIdle());
    System.out.println("pool.getCreatedCount: " + pool.getCreatedCount());
    System.out.println("pool.getReturnedCount: " + pool.getReturnedCount());
    System.out.println("pool.getBorrowedCount: " + pool.getBorrowedCount());
    System.out.println("pool.getDestroyedCount: " + pool.getDestroyedCount());
  }

  public static void printResults(
    String testName,
    int numOfThreads,
    int commandsAmount,
    double duration,
    int maxConnectionInPool
  ) {
    System.out.println("Test Name: " + testName);
    System.out.println(
      "\tTPS: " + Double.valueOf(commandsAmount / duration).toString()
    );
    System.out.println("\tThreads: " + numOfThreads);
    System.out.println(
      "\tCommandsPerThread: " +
      Double.valueOf(commandsAmount / numOfThreads).toString()
    );
    System.out.println("\tConnection Pool Size: " + maxConnectionInPool);
    System.out.println("\tDuration: " + duration);
  }

  public static class WorkingThread extends Thread {

    GenericObjectPool<StatefulRedisConnection<String, String>> _pool;

    public WorkingThread(
      GenericObjectPool<StatefulRedisConnection<String, String>> pool
    ) {
      _pool = pool;
    }

    public void run() {
      ThreadLocalRandom rand = ThreadLocalRandom.current();
      try {
        for (Integer i = 0; i < COMMANDS_PER_TEST / NUM_OF_THREADS; i++) {
          StatefulRedisConnection<String, String> connection = _pool.borrowObject();
          RedisCommands<String, String> syncCommands = connection.sync();
          runSyncQueryToServer(rand, syncCommands);
          _pool.returnObject(connection);
        }
      } catch (Exception t) {
        System.out.println("Error " + t);
      }
    }
  }

  public static void scenarioThreadsWithConnPool(
    int amountOfThreads,
    int maxClientInPool
  ) {
    NUM_OF_THREADS = amountOfThreads;

    try {
      RedisURI redisUri = RedisURI.Builder.redis(HOST).withDatabase(0).build();
      RedisClient redis = RedisClient.create(redisUri);

      GenericObjectPool<StatefulRedisConnection<String, String>> pool = ConnectionPoolSupport.createGenericObjectPool(
        () -> redis.connect(),
        new GenericObjectPoolConfig()
      );

      Integer actualPoolSize;
      if (maxClientInPool > 0) {
        actualPoolSize = maxClientInPool;
      } else {
        actualPoolSize = amountOfThreads;
      }
      pool.setMaxTotal(actualPoolSize);
      pool.setMaxIdle(actualPoolSize);

      WorkingThread[] myThreads = new WorkingThread[amountOfThreads];

      long start = System.currentTimeMillis();
      int number_of_threads = amountOfThreads;
      for (int ii = 0; ii < number_of_threads; ++ii) {
        myThreads[ii] = new WorkingThread(pool);
        myThreads[ii].start();
      }

      for (int ii = 0; ii < number_of_threads; ++ii) {
        myThreads[ii].join();
      }

      long end = System.currentTimeMillis();
      double duration = ((double) (end - start)) / 1000;

      printResults(
        "scenario_ThreadWithClient",
        amountOfThreads,
        COMMANDS_PER_TEST,
        duration,
        actualPoolSize
      );

      // terminating
      pool.close();
      redis.shutdown();
    } catch (Throwable t) {
      System.out.println("Error" + t);
    }
  }

  public enum PipelineMode {
    PipelineBatching,
    PipelineTransaction,
    PipelineOnly,
  }

  public static void scenarioPipelineTypes(PipelineMode pipelineMode) {
    RedisClient redis = null;
    StatefulRedisConnection<String, String> connection = null;
    try {
      RedisURI redisUri = RedisURI.Builder.redis(HOST).withDatabase(0).build();

      redis = RedisClient.create(redisUri);
      connection = redis.connect();

      RedisAsyncCommands<String, String> commands = connection.async();

      if (PipelineMode.PipelineBatching == pipelineMode) {
        commands.setAutoFlushCommands(false);
      }

      long start = System.currentTimeMillis();

      for (int i = 0; i < COMMANDS_PER_TEST / PIPELINE_SIZE; ++i) {
        List<RedisFuture<String>> futures = new ArrayList<RedisFuture<String>>();

        if (PipelineMode.PipelineTransaction == pipelineMode) {
          futures.add(commands.multi());
        }

        for (int ii = 0; ii < PIPELINE_SIZE; ++ii) {
          addAsyncQueryToConnection(r, futures, commands);
        }

        if (PipelineMode.PipelineTransaction == pipelineMode) {
          try {
            RedisFuture<TransactionResult> exec = commands.exec();
            exec.get(5, TimeUnit.SECONDS);
          } catch (Exception e) {
            System.out.println("Running Transaction threw: " + e);
          }
        } else {
          if (PipelineMode.PipelineBatching == pipelineMode) {
            // write all commands to the transport layer
            commands.flushCommands();
          }

          // synchronization example: Wait until all futures complete
          LettuceFutures.awaitAll(
            5,
            TimeUnit.SECONDS,
            futures.toArray(new RedisFuture[futures.size()])
          );
        }
      }
      long end = System.currentTimeMillis();
      double duration = ((double) (end - start)) / 1000;
      System.out.println("Test Name: Pipeline - " + pipelineMode.name());
      System.out.println("\tPipeline size: " + PIPELINE_SIZE);
      System.out.println("\tQueries in test: " + COMMANDS_PER_TEST);
      System.out.println(
        "\tTPS: " + Double.valueOf(COMMANDS_PER_TEST / duration).toString()
      );
      System.out.println("\tDuration: " + duration);
    } finally {
      if (connection != null) {
        connection.close();
      }
    }

    if (redis != null) {
      redis.shutdown();
    }
  }

  public static void main(String[] args) {
    warmup();

    COMMANDS_PER_TEST = 30000000;

    List<Integer> numOfThreadsList = new ArrayList<Integer>(
      Arrays.asList(1, 10, 20, 50, 100, 1000)
    );
    List<Integer> poolSizeList = new ArrayList<Integer>(Arrays.asList(10, 200));

    for (Integer numOfThreads : numOfThreadsList) {
      for (Integer poolSize : poolSizeList) {
        scenarioThreadsWithConnPool(numOfThreads, poolSize);
      }
    }

    for (PipelineMode mode : PipelineMode.values()) {
      COMMANDS_PER_TEST = 1000 * 1000;
      PIPELINE_SIZE = 10;
      scenarioPipelineTypes(mode);
      COMMANDS_PER_TEST = 30 * 1000 * 1000;
      PIPELINE_SIZE = 100;
      scenarioPipelineTypes(mode);
      COMMANDS_PER_TEST = 50 * 1000 * 1000;
      PIPELINE_SIZE = 500;
      scenarioPipelineTypes(mode);
      COMMANDS_PER_TEST = 100 * 1000 * 1000;
      PIPELINE_SIZE = 1000;
      scenarioPipelineTypes(mode);
      COMMANDS_PER_TEST = 100 * 1000 * 1000;
      PIPELINE_SIZE = 5000;
      scenarioPipelineTypes(mode);
      COMMANDS_PER_TEST = 100 * 1000 * 1000;
      PIPELINE_SIZE = 10000;
      scenarioPipelineTypes(mode);
      COMMANDS_PER_TEST = 100 * 1000 * 1000;
      PIPELINE_SIZE = 100000;
      scenarioPipelineTypes(mode);
    }
  }
}
