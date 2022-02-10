package com.amazon.redisbenchmark.jedis;
import java.util.Random;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

public class MainJedis {

  public static final String HOST =
    "localhost";
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
    System.out.println("Warmup started");
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

  public static void main(String[] args) {
    
    warmup();

    COMMANDS_PER_TEST = 50000000;
    
    
    MainJedis.PIPELINE_SIZE = 5;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 10;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 20;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 50;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 100;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 200;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 500;
    scenarioPipeline();
    MainJedis.PIPELINE_SIZE = 1000;
    scenarioPipeline();
    

    MainJedis.PIPELINE_SIZE = 5;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 10;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 20;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 50;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 100;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 200;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE = 500;
    scenarioTransaction();
    MainJedis.PIPELINE_SIZE =1000;
    scenarioTransaction();
  }
}
