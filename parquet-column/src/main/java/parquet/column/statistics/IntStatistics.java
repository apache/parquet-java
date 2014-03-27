package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class IntStatistics extends Statistics{

  private int max;
  private int min;

  @Override
  public void updateStats(int value) {
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
      IntStatistics intStats = (IntStatistics)stats;
      return (max == intStats.getMax()) &&
             (min == intStats.getMin()) &&
             (this.getNumNulls() == intStats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      IntStatistics intStats = (IntStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = intStats.getMax();
        min = intStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(intStats.getMax());
        updateMin(intStats.getMin());
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
    return BytesUtils.intToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(min);
  }

  public void updateMax(int value){
    if (value > max) { max = value; }
  }

  public void updateMin(int value) {
    if (value < min) { min = value; }
  }

  public int getMax() {
    return max;
  }

  public int getMin() {
    return min;
  }

  public void setMinMax(int min, int max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}
