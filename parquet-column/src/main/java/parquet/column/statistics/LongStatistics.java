package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class LongStatistics extends Statistics{

  private long max;
  private long min;

  @Override
  public void updateStats(long value) {
    if (this.isEmpty()) {
      max = value;
      min = value;
      this.markAsNotEmpty();
    } else {
      updateMax(value);
      updateMin(value);
    }
  }

  @Override
  public boolean equals(Statistics stats) {
    if (this.getClass() == stats.getClass()) {
      LongStatistics longStats = (LongStatistics)stats;
      return (max == longStats.getMax()) &&
             (min == longStats.getMin()) &&
             (this.getNumNulls() == longStats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      LongStatistics longStats = (LongStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = longStats.getMax();
        min = longStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(longStats.getMax());
        updateMin(longStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToInt(maxBytes);
    min = BytesUtils.bytesToInt(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(min);
  }
  public void updateMax(long value){
    if (value > max) { max = value; }
  }

  public void updateMin(long value) {
    if (value < min) { min = value; }
  }

  public long getMax() {
    return max;
  }

  public long getMin() {
    return min;
  }

  public void setMinMax(long min, long max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}