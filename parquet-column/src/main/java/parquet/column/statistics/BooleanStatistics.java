package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class BooleanStatistics extends Statistics{

  private boolean max;
  private boolean min;

  @Override
  public void updateStats(boolean value) {
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
      BooleanStatistics booleanStats = (BooleanStatistics)stats;
      return (max == booleanStats.getMax()) &&
             (min == booleanStats.getMin()) &&
             (this.getNumNulls() == stats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      BooleanStatistics boolStats = (BooleanStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = boolStats.getMax();
        min = boolStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(boolStats.getMax());
        updateMin(boolStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = BytesUtils.bytesToBool(maxBytes);
    min = BytesUtils.bytesToBool(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.booleanToBytes(max);
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.booleanToBytes(min);
  }

  public void updateMax(boolean value){
    if (!max && value) { max = value; }
  }

  public void updateMin(boolean value) {
    if (min && !value) { min = value; }
  }

  public boolean getMax() {
    return max;
  }

  public boolean getMin() {
    return min;
  }

  public void setMinMax(boolean min, boolean max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}