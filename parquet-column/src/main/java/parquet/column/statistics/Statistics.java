package parquet.column.statistics;

import parquet.io.api.Binary;

/**
 * Statistics class to keep track of statistics in parquet pages and column chunks
 *
 * @author Katya Gonina
 */
public abstract class Statistics {

  private boolean firstValueAccountedFor;
  private long num_nulls;

  public Statistics() {
    firstValueAccountedFor = false;
    num_nulls = 0;
  }

  public void updateStats(int value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(long value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(float value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(double value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(boolean value) {
    throw new UnsupportedOperationException();
  }

  public void updateStats(Binary value) {
    throw new UnsupportedOperationException();
  }

  abstract boolean equals(Statistics stats);

  abstract public void mergeStatistics(Statistics stats);

  abstract public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes);

  abstract public byte[] getMaxBytes();

  abstract public byte[] getMinBytes();

  public void incrementNumNulls() {
    num_nulls++ ;
  }

  public void incrementNumNulls(long increment) {
    num_nulls += increment ;
  }
  public long getNumNulls() {
    return num_nulls;
  }

  public boolean isEmpty() {
    return !firstValueAccountedFor;
  }

  public void markAsNotEmpty() {
    firstValueAccountedFor = true;
  }

  public void setNumNulls(long nulls) {
    num_nulls = nulls;
  }
}

