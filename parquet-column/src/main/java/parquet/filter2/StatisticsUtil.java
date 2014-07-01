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

/**
 * {@link parquet.column.statistics.Statistics} is not a generic class, which makes working with
 * it and its subclasses difficult and tedious.
 *
 * This class has some helpers for getting around that.
 *
 * TODO(alexlevenson): make Statistics generic (or conform to a generic view of some kind?)
 */
public final class StatisticsUtil {
  private StatisticsUtil() { }

  /**
   * A struct representing the comparison between some value v,
   * and the min / max of a statistics object.
   */
  public static final class MinMaxComparison {
    private final int minCmp;
    private final int maxCmp;

    private MinMaxComparison(int minCmp, int maxCmp) {
      this.minCmp = minCmp;
      this.maxCmp = maxCmp;
    }

    /**
     * represents value.compareTo(min)
     */
    public int getMinCmp() {
      return minCmp;
    }

    /**
     * represents value.compareTo(max)
     */
    public int getMaxCmp() {
      return maxCmp;
    }
  }

  /**
   * Returns the result of comparing rawValue to the min and max in rawStats
   *
   * @param clazz The class representing the type of statistics object rawStats is.
   *              eg: Integer.class, Long.class, Binary.class etc
   *
   * @param rawValue the value to compare to
   * @param rawStats a Statistics object that must be of matching type to clazz
   *                 eg: IntStatistics for Integer.class
   */
  public static <T> MinMaxComparison compareMinMax(Class<T> clazz, T rawValue, Statistics rawStats) {

    if (clazz.equals(Integer.class)) {
      IntStatistics stats = (IntStatistics) rawStats;
      Integer value = (Integer) rawValue;
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    if (clazz.equals(Long.class)) {
      LongStatistics stats = (LongStatistics) rawStats;
      Long value = (Long) rawValue;
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    if (clazz.equals(Float.class)) {
      FloatStatistics stats = (FloatStatistics) rawStats;
      Float value = (Float) rawValue;
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    if (clazz.equals(Double.class)) {
      DoubleStatistics stats = (DoubleStatistics) rawStats;
      Double value = (Double) rawValue;
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    if (clazz.equals(Boolean.class)) {
      BooleanStatistics stats = (BooleanStatistics) rawStats;
      Boolean value = (Boolean) rawValue;
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    if (clazz.equals(Binary.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      Binary value = (Binary) rawValue;
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    if (clazz.equals(String.class)) {
      BinaryStatistics stats = (BinaryStatistics) rawStats;
      String strValue = (String) rawValue;
      Binary value = Binary.fromByteBuffer(BytesUtils.UTF8.encode(strValue));
      return new MinMaxComparison(
          value.compareTo(stats.getMin()),
          value.compareTo(stats.getMax())
      );
    }

    throw new IllegalArgumentException("Encountered unknown filter column type: " + clazz.getName());
  }

  /**
   * Applies a {@link parquet.filter2.UserDefinedPredicates.UserDefinedPredicate}
   * to a statistics object, paying attention to whether the predicate is supposed to
   * be logically inverted.
   *
   * @param clazz The class representing the type of statistics object rawStats is.
   *              eg: Integer.class, Long.class, Binary.class etc
   *
   * @param rawUdp the UserDefinedPredicate to apply, must be of matching type to clazz
   *               eg: IntUserDefinedPredicate for Integer.class
   *
   * @param rawStats a Statistics object that must be of matching type to clazz
   *                 eg: IntStatistics for Integer.class
   *
   * @param inverted if true, the predicate has been logically inverted by the not operator, and
   *                 the inverseCanDrop method will be called instead of the canDrop method.
   *
   * @return whether the records represented by rawStats can be dropped according to the provided predicate
   */
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
