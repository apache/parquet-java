package redelm.io;

public abstract class RecordReader<T> {

  /**
   * reads one record and returns it
   * @return the materialized record
   */
  public abstract T read();

  /**
   * reads count record and writes them in the provided array
   * @param records the target
   * @param count how many to read
   */
  public abstract void read(T[] records, int count);

}