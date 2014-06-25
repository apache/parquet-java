package parquet.filter2;

import parquet.bytes.BytesUtils;
import parquet.column.statistics.BinaryStatistics;
import parquet.column.statistics.BooleanStatistics;
import parquet.column.statistics.DoubleStatistics;
import parquet.column.statistics.FloatStatistics;
import parquet.column.statistics.IntStatistics;
import parquet.column.statistics.LongStatistics;
import parquet.column.statistics.Statistics;
import parquet.filter2.UserDefinedPredicates.BinaryUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.DoubleUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.FloatUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.IntUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.LongUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.StringUserDefinedPredicate;
import parquet.filter2.UserDefinedPredicates.UserDefinedPredicate;
import parquet.io.api.Binary;

public final class StatisticsUtil {
  private StatisticsUtil() { }

  public static final class MinMaxComparison {
    private final int minCmp;
    private final int maxCmp;

    private MinMaxComparison(int minCmp, int maxCmp) {
      this.minCmp = minCmp;
      this.maxCmp = maxCmp;
    }

    public int getMinCmp() {
      return minCmp;
    }

    public int getMaxCmp() {
      return maxCmp;
    }
  }

  public static <T> MinMaxComparison compareMinMax(Class<T> clazz, T rawValue, Statistics rawStats) {

    if (clazz.equals(Integer.class)) {
      IntStatistics stats = (IntStatistics) rawStats;
      int value = (Integer) rawValue;
      return new MinMaxComparison(
          Integer.compare(value, stats.getMin()),
          Integer.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Long.class)) {
      LongStatistics stats = (LongStatistics) rawStats;
      long value = (Long) rawValue;
      return new MinMaxComparison(
          Long.compare(value, stats.getMin()),
          Long.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Float.class)) {
      FloatStatistics stats = (FloatStatistics) rawStats;
      float value = (Float) rawValue;
      return new MinMaxComparison(
          Float.compare(value, stats.getMin()),
          Float.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Double.class)) {
      DoubleStatistics stats = (DoubleStatistics) rawStats;
      double value = (Double) rawValue;
      return new MinMaxComparison(
          Double.compare(value, stats.getMin()),
          Double.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Boolean.class)) {
      BooleanStatistics stats = (BooleanStatistics) rawStats;
      boolean value = (Boolean) rawValue;
      return new MinMaxComparison(
          Boolean.compare(value, stats.getMin()),
          Boolean.compare(value, stats.getMax())
      );
    }

    if (clazz.equals(Binary.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      Binary value = (Binary) rawValue;

      int minCmp = value.compareTo(stats.getMin());
      int maxCmp = value.compareTo(stats.getMax());
      return new MinMaxComparison(minCmp, maxCmp);
    }

    if (clazz.equals(String.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      String strValue = (String) rawValue;
      Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));

      int minCmp = value.compareTo(stats.getMin());
      int maxCmp = value.compareTo(stats.getMax());
      return new MinMaxComparison(minCmp, maxCmp);
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  public static <T> boolean applyUdpMinMax(Class<T> clazz, UserDefinedPredicate<T> rawUdp, Statistics rawStats, boolean inverted) {

    if (clazz.equals(Integer.class)) {
      IntStatistics stats = (IntStatistics) rawStats;
      IntUserDefinedPredicate udp = (IntUserDefinedPredicate) rawUdp;
      if (inverted) {
        return udp.inverseCanDrop(stats.getMin(), stats.getMax());
      } else {
        return udp.canDrop(stats.getMin(), stats.getMax());
      }
    }

    if (clazz.equals(Long.class)) {
      LongStatistics stats = (LongStatistics) rawStats;
      LongUserDefinedPredicate udp = (LongUserDefinedPredicate) rawUdp;
      if (inverted) {
        return udp.inverseCanDrop(stats.getMin(), stats.getMax());
      } else {
        return udp.canDrop(stats.getMin(), stats.getMax());
      }
    }

    if (clazz.equals(Float.class)) {
      FloatStatistics stats = (FloatStatistics) rawStats;
      FloatUserDefinedPredicate udp = (FloatUserDefinedPredicate) rawUdp;
      if (inverted) {
        return udp.inverseCanDrop(stats.getMin(), stats.getMax());
      } else {
        return udp.canDrop(stats.getMin(), stats.getMax());
      }
    }

    if (clazz.equals(Double.class)) {
      DoubleStatistics stats = (DoubleStatistics) rawStats;
      DoubleUserDefinedPredicate udp = (DoubleUserDefinedPredicate) rawUdp;
      if (inverted) {
        return udp.inverseCanDrop(stats.getMin(), stats.getMax());
      } else {
        return udp.canDrop(stats.getMin(), stats.getMax());
      }
    }

    if (clazz.equals(Binary.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      BinaryUserDefinedPredicate udp = (BinaryUserDefinedPredicate) rawUdp;
      if (inverted) {
        return udp.inverseCanDrop(stats.getMin(), stats.getMax());
      } else {
        return udp.canDrop(stats.getMin(), stats.getMax());
      }
    }

    if (clazz.equals(String.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      StringUserDefinedPredicate udp = (StringUserDefinedPredicate) rawUdp;
      if (inverted) {
        return udp.inverseCanDrop(stats.getMin().toStringUsingUTF8(), stats.getMax().toStringUsingUTF8());
      } else {
        return udp.canDrop(stats.getMin().toStringUsingUTF8(), stats.getMax().toStringUsingUTF8());
      }
    }

    throw new IllegalArgumentException("Encountered unknown filter column type for user defined predicate " + clazz);
  }

}
