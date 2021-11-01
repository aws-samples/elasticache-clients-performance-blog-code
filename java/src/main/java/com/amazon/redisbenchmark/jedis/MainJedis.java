package com.amazon.redisbenchmark.jedis;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.time.Duration;
import java.util.Random;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

public class MainJedis {

  public static final String HOST =
    "redistestclients.p2flvq.ng.0001.use1.cache.amazonaws.com";
  public static final int PORT = 6379;
  public static final int NUM_GET_KEYS = 3750000;
  public static final int NUM_SET_KEYS = (int) (NUM_GET_KEYS * 0.8);
    public static final int NORMAL_MEAN = 1024;
  public static final int NORMAL_STD = 400;
  public static final double PROBABILITY_OF_GET = 0.8;
  public static int PIPELINE_SIZE = 1000; // This params might change during tests
  public static int COMMANDS_PER_TEST = 1000 * 1000 * 10; //This params might change during tests
  public static Random r;

  static {
    r = new Random();
  }

  private static Integer getSetKey() {
    return r.nextInt(NUM_SET_KEYS) + 1;
  }

  private static Integer getGetKey() {
    return r.nextInt(NUM_GET_KEYS) + 1;
  }

  private static int getValueSize() {
    return (int) Math.round(r.nextGaussian() * NORMAL_STD + NORMAL_MEAN);
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
  private static Boolean shouldSet() {
    return (PROBABILITY_OF_GET < r.nextFloat());
  }

  private static void warmup() {
    try (Jedis jedis = new Jedis(HOST, PORT, 15000, 15000))
    {
      jedis.connect();
      jedis.flushAll();

      final int warmupPipelineSize = 100;
      for (int i = 0; i < NUM_SET_KEYS / warmupPipelineSize ; ++i) 
      {
        Pipeline p = jedis.pipelined();
        for (int j = 0; j < warmupPipelineSize; ++j) {
          p.set(Integer.valueOf(i * warmupPipelineSize + j).toString() ,getValueString(getValueSize()));
        }
        p.sync();
      }        
    }      
    catch (Exception e)
    {
      System.out.println("Warmup threw: " + e);
    }
    System.out.println("Warmup done");
  }

  public static void scenarioTransaction() {
    try (Jedis jedis = new Jedis(HOST, PORT, 15000, 15000)) {
      jedis.connect();

      long start = System.currentTimeMillis();

      for (int i = 0; i < COMMANDS_PER_TEST / PIPELINE_SIZE; ++i) {
        Transaction t = jedis.multi();
        for (int ii = 0; ii < PIPELINE_SIZE; ++ii) {
          if (shouldSet()) {
            t.set(getSetKey().toString(), getValueString(getValueSize()));
          } else {
            t.get(getGetKey().toString());
          }
        }
        t.exec();
      }
      long end = System.currentTimeMillis();
      double duration = ((double) (end - start)) / 1000;
      printResults("Transaction", duration);      
    } 
    catch (Exception e) 
    {
      System.out.println("Got exception: " + e);
    }
  }

  public static void scenarioPipeline() {
    try (Jedis jedis = new Jedis(HOST, PORT, 15000, 15000)) {
      jedis.connect();
      long start = System.currentTimeMillis();

      for (int i = 0; i < COMMANDS_PER_TEST / PIPELINE_SIZE; ++i) {
        Pipeline p = jedis.pipelined();
        for (int ii = 0; ii < PIPELINE_SIZE; ++ii) {
          if (shouldSet()) {
            p.set(getSetKey().toString(), getValueString(getValueSize()));
          } else {
            p.get(getGetKey().toString());
          }
        }
        p.sync();
      }
      long end = System.currentTimeMillis();
      double duration = ((double) (end - start)) / 1000;
      printResults("Pipeline", duration);
    } 
    catch (Exception e) 
    {
      System.out.println("Got exception: " + e);
    }
  }

  public static void printResults(
    String testName,
    double duration
  ) {
    System.out.println("Test Name: " + testName);
    System.out.println("\tPipeline size: " + PIPELINE_SIZE);
    System.out.println(
      "\tTPS: " + Double.valueOf(COMMANDS_PER_TEST / duration).toString()
    );
    System.out.println("\tDuration: " + duration);
  }

  public static void printResultsThread(
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

    JedisPool _jedisPool;
    int _amountOfThreads;

    public WorkingThread(JedisPool jedisPool, int amountOfThreads) {
      _jedisPool = jedisPool;
      _amountOfThreads = amountOfThreads;
    }

    public void run() {
      Double NumOfQueriesToRun = Double.valueOf(
        MainJedis.COMMANDS_PER_TEST / _amountOfThreads
      );

      try {
        for (Integer i = 0; i < NumOfQueriesToRun; i++) {
          Jedis jedis = _jedisPool.getResource();
          jedis.connect();
          if (shouldSet()) {
            jedis.set(getSetKey().toString(), getValueString(getValueSize()));
          } else {
            jedis.get(getGetKey().toString());
          }
          jedis.close();
        }
      } catch (Throwable t) {
        System.out.println("Error2: " + t);
      } 
    }
  }

  private static JedisPoolConfig buildPoolConfig() {
    final JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMinIdle(16);
    poolConfig.setTestOnBorrow(false);
    poolConfig.setTestOnReturn(false);
    poolConfig.setTestWhileIdle(false);
    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
    poolConfig.setTimeBetweenEvictionRunsMillis(
      Duration.ofSeconds(30).toMillis()
    );
    poolConfig.setBlockWhenExhausted(true);
    return poolConfig;
  }

  public static void scenarioThreadsWithConnPool(
    int amountOfThreads,
    int maxConnectionsInPool
  ) {

    final JedisPoolConfig poolConfig = buildPoolConfig();
    if (maxConnectionsInPool > 0) {
      poolConfig.setMaxTotal(amountOfThreads);
      poolConfig.setMaxIdle(amountOfThreads);
    } else {
      //else we don't limit the pool, the default value in our config func is 128, so negative value means no limit.
      poolConfig.setMaxTotal(maxConnectionsInPool);
      poolConfig.setMaxIdle(maxConnectionsInPool);
    }

    try (JedisPool jedisPool = new JedisPool(poolConfig, HOST, PORT)) {
      Jedis jedis = jedisPool.getResource();
      jedis.connect();
      jedis.close();

      WorkingThread[] myThreads = new WorkingThread[amountOfThreads];

      long start = System.currentTimeMillis();
      for (int ii = 0; ii < amountOfThreads; ++ii) {
        myThreads[ii] = new WorkingThread(jedisPool, amountOfThreads);
        myThreads[ii].start();
      }

      for (int ii = 0; ii < amountOfThreads; ++ii) {
        myThreads[ii].join();
      }

      long end = System.currentTimeMillis();
      double duration = ((double) (end - start)) / 1000;

      printResultsThread(
        "scenario_ThreadWithClient",
        amountOfThreads,
        COMMANDS_PER_TEST,
        duration,
        maxConnectionsInPool
      );

      // terminating
      jedisPool.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }


  public static void main(String[] args) {
    
    warmup();

    COMMANDS_PER_TEST = 50000000;
    
    MainJedis.PIPELINE_SIZE = 10;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 100;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 1000;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 10000;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 100000;
    scenarioPipeline();

    
    MainJedis.PIPELINE_SIZE = 10;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 100;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 1000;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 10000;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 100000;
    scenarioTransaction();

    
    
    COMMANDS_PER_TEST = 50000000;

    List<Integer> numOfThreadsList = new ArrayList<Integer>(Arrays.asList(1, 10, 20, 50, 100, 1000));
    List<Integer> poolSizeList = new ArrayList<Integer>(Arrays.asList(10, 200));

    for(Integer numOfThreads : numOfThreadsList)
    {
      for(Integer poolSize : poolSizeList)
      {
        scenarioThreadsWithConnPool(numOfThreads, poolSize);  
      }
    }
   
  }
}
