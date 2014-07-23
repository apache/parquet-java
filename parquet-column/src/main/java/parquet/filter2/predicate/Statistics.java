package parquet.filter2.predicate;

import static parquet.Preconditions.checkNotNull;

/**
 * Contains statistics about a group of records
 */
public class Statistics<T> {
  private final T min;
  private final T max;

  public Statistics(T min, T max) {
    this.min = checkNotNull(min, "min");
    this.max = checkNotNull(max, "max");
  }

  public T getMin() {
    return min;
  }

  public T getMax() {
    return max;
  }
}
