package parquet.column.statistics;

import parquet.bytes.BytesUtils;

public class FloatStatistics extends Statistics{

  private float max;
  private float min;

  @Override
  public void updateStats(float value) {
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
      FloatStatistics floatStats = (FloatStatistics)stats;
      return (max == floatStats.getMax()) &&
             (min == floatStats.getMin()) &&
             (this.getNumNulls() == floatStats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      FloatStatistics floatStats = (FloatStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = floatStats.getMax();
        min = floatStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(floatStats.getMax());
        updateMin(floatStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Float.intBitsToFloat(BytesUtils.bytesToInt(maxBytes));
    min = Float.intBitsToFloat(BytesUtils.bytesToInt(minBytes));
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return BytesUtils.intToBytes(Float.floatToIntBits(max));
  }

  @Override
  public byte[] getMinBytes() {
    return BytesUtils.intToBytes(Float.floatToIntBits(min));
  }

  public void updateMax(float value){
    if (value > max) { max = value; }
  }

  public void updateMin(float value) {
    if (value < min) { min = value; }
  }

  public float getMax() {
    return max;
  }

  public float getMin() {
    return min;
  }

  public void setMinMax(float min, float max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }

}
