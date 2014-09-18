package parquet.hadoop.api;

import org.apache.hadoop.conf.Configuration;

import parquet.io.api.RecordConsumer;

/**
 *
 * Helps composing write supports
 *
 * @author Julien Le Dem
 *
 * @param <T>
 */
public class DelegatingWriteSupport<T> extends WriteSupport<T> {

  private final WriteSupport<T> delegate;

  public DelegatingWriteSupport(WriteSupport<T> delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public WriteSupport.WriteContext init(Configuration configuration) {
    return delegate.init(configuration);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    delegate.prepareForWrite(recordConsumer);
  }

  @Override
  public void write(T record) {
    delegate.write(record);
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    return delegate.finalizeWrite();
  }

  @Override
  public String toString() {
    return getClass().getName() + "(" + delegate.toString() + ")";
  }
}
