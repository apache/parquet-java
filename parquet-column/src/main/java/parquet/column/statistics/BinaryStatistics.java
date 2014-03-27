package parquet.column.statistics;

import parquet.io.api.Binary;

public class BinaryStatistics extends Statistics{

  private Binary max;
  private Binary min;

  @Override
  public void updateStats(Binary value) {
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
      BinaryStatistics binaryStats = (BinaryStatistics)stats;
      return (max.equals(binaryStats.getMax())) &&
             (min.equals(binaryStats.getMin())) &&
             (this.getNumNulls() == binaryStats.getNumNulls());
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void mergeStatistics(Statistics stats) {
    if (stats.isEmpty()) return;

    if (this.getClass() == stats.getClass()) {
      BinaryStatistics binaryStats = (BinaryStatistics)stats;
      if (this.isEmpty()) {
        this.setNumNulls(stats.getNumNulls());
        max = binaryStats.getMax();
        min = binaryStats.getMin();
        this.markAsNotEmpty();
      } else {
        incrementNumNulls(stats.getNumNulls());
        updateMax(binaryStats.getMax());
        updateMin(binaryStats.getMin());
      }
    } else {
      throw new StatisticsClassException(this.getClass().toString(), stats.getClass().toString());
    }
  }

  @Override
  public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
    max = Binary.fromByteArray(maxBytes);
    min = Binary.fromByteArray(minBytes);
    this.markAsNotEmpty();
  }

  @Override
  public byte[] getMaxBytes() {
    return max.getBytes();
  }

  @Override
  public byte[] getMinBytes() {
    return min.getBytes();
  }

  public void updateMax(Binary value){
    //TODO: what's a better comparison without converting to string?
    if (max.toStringUsingUTF8().compareTo(value.toStringUsingUTF8()) < 0) { max = value; }
  }

  public void updateMin(Binary value) {
    //TODO: what's a better comparison without converting to string?
    if (min.toStringUsingUTF8().compareTo(value.toStringUsingUTF8()) > 0) { min = value; }
  }

  public Binary getMax() {
    return max;
  }

  public Binary getMin() {
    return min;
  }

  public void setMinMax(Binary min, Binary max) {
    this.max = max;
    this.min = min;
    this.markAsNotEmpty();
  }
}