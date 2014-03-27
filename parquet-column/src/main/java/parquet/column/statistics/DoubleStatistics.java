package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class DoubleStatistics extends Statistics{

  private double max;
  private double min;

  @Override
  public void updateStats(double value) {
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
      DoubleStatistics doubleStats = (DoubleStatistics)stats;
      return (max == doubleStats.getMax()) &&
             (min == doubleStats.getMin()) &&
             (this.getNumNulls() == doubleStats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      DoubleStatistics doubleStats = (DoubleStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = doubleStats.getMax();
        min = doubleStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(doubleStats.getMax());
        updateMin(doubleStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Double.longBitsToDouble(BytesUtils.bytesToLong(maxBytes));
    min = Double.longBitsToDouble(BytesUtils.bytesToLong(minBytes));
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.longToBytes(Double.doubleToLongBits(max));
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.longToBytes(Double.doubleToLongBits(min));
  }

  public void updateMax(double value){
    if (value > max) { max = value; }
  }

  public void updateMin(double value) {
    if (value < min) { min = value; }
  }

  public double getMax() {
    return max;
  }

  public double getMin() {
    return min;
  }

  public void setMinMax(double min, double max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}