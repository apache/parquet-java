package parquet.hadoop.util;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 * Created with IntelliJ IDEA.
 * User: tdeng
 * Date: 8/15/13
 * Time: 10:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class BenchmarkCounter {
  public static final String ENABLE_BYTES_READ_COUNTER = "parquet.benchmark.bytes.read";
  public static final String ENABLE_BYTES_TOTAL_COUNTER = "parquet.benchmark.bytes.total";
  public static final String ENABLE_TIME_READ_COUNTER = "parquet.benchmark.time.read";

  public static final String COUNTER_GROUP_NAME="parquet";
  static Counter bytesReadCounter = null;
  static Counter totalBytesCounter = null;
  static Counter timeCounter = null;

  //TODO: read flags from configuration
  public static void initCounterFromContext(TaskInputOutputContext<?, ?, ?, ?> context) {

    bytesReadCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, "byteread", ENABLE_BYTES_READ_COUNTER);
    totalBytesCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, "totalbyte", ENABLE_BYTES_TOTAL_COUNTER);
    timeCounter = getCounterWhenFlagIsSet(context, COUNTER_GROUP_NAME, "readingtime", ENABLE_TIME_READ_COUNTER);
  }

  private static Counter getCounterWhenFlagIsSet(TaskInputOutputContext<?, ?, ?, ?> context, String groupName, String counterName, String counterFlag) {
    if (context.getConfiguration().getBoolean(counterFlag, true)) {
      return ContextUtil.getCounter(context, groupName, counterName);
    } else {
      return null;
    }
  }

  //TODO handle when the variable is empty
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
