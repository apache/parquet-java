package parquet.hadoop.util;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class BenchmarkCounter {
  public static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  public static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  public static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";
  public static final String BYTES_READ_COUNTER_NAME="bytesread";
  public static final String BYTES_TOTAL_COUNTER_NAME="bytestotal";
  public static final String TIME_READ_COUNTER_NAME="timeread";


  public static final String COUNTER_GROUP_NAME="parquet";
  static Counter bytesReadCounter = null;
  static Counter totalBytesCounter = null;
  static Counter timeCounter = null;

  public static void initCounterFromContext(TaskInputOutputContext<?, ?, ?, ?> context) {
    bytesReadCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, BYTES_READ_COUNTER_NAME, ENABLE_BYTES_READ_COUNTER);
    totalBytesCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, BYTES_TOTAL_COUNTER_NAME, ENABLE_BYTES_TOTAL_COUNTER);
    timeCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, TIME_READ_COUNTER_NAME, ENABLE_TIME_READ_COUNTER);
  }

  private static Counter getCounterWhenFlagIsSet(TaskInputOutputContext<?, ?, ?, ?> context, String groupName, String counterName, String counterFlag) {
    if (context.getConfiguration().getBoolean(counterFlag, true)) {
      return ContextUtil.getCounter(context, groupName, counterName);
    } else {
      return null;
    }
  }

  public static void incrementTotalBytes(long val) {
    if (totalBytesCounter != null) {
      totalBytesCounter.increment(val);
    }
  }

  public static void incrementBytesRead(long val) {
    if (bytesReadCounter != null) {
      bytesReadCounter.increment(val);
    }
  }

  public static void incrementTime(long val) {
    if (timeCounter != null) {
      timeCounter.increment(val);
    }
  }

}
