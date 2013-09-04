package parquet.hadoop.util.counters;

/**
 * Interface for counters in mapred/mapreduce package of hadoop
 * @author Tianshuo Deng
 */
public interface ICounter {
  public void increment(long val);
  public long getCount();

}
