package parquet.hadoop.util;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class BenchmarkCounter {

  private static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  private static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  private static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  private static final String COUNTER_GROUP_NAME="parquet";
  private static final String BYTES_READ_COUNTER_NAME="bytesread";
  private static final String BYTES_TOTAL_COUNTER_NAME="bytestotal";
  private static final String TIME_READ_COUNTER_NAME="timeread";

  private static Counter bytesReadCounter = null;
  private static Counter totalBytesCounter = null;
  private static Counter timeCounter = null;

  public static void initCounterFromContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    bytesReadCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, BYTES_READ_COUNTER_NAME, ENABLE_BYTES_READ_COUNTER);
    totalBytesCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, BYTES_TOTAL_COUNTER_NAME, ENABLE_BYTES_TOTAL_COUNTER);
    timeCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, TIME_READ_COUNTER_NAME, ENABLE_TIME_READ_COUNTER);
  }

  private static Counter getCounterWhenFlagIsSet(TaskInputOutputContext<?, ?, ?, ?> context, String groupName, String counterName, String counterFlag) {
    if (ContextUtil.getConfiguration(context).getBoolean(counterFlag, true)) {
      return ContextUtil.getCounter(context, groupName, counterName);
    } else {
      return null;
    }
  }

  public static void incrementTotalBytes(long val) {
    if (totalBytesCounter != null) {
      ContextUtil.incrementCounter(totalBytesCounter,val);
    }
  }

  public static void incrementBytesRead(long val) {
    if (bytesReadCounter != null) {
      ContextUtil.incrementCounter(bytesReadCounter, val);
    }
  }

  public static void incrementTime(long val) {
    if (timeCounter != null) {
      ContextUtil.incrementCounter(timeCounter, val);
    }
  }

}
