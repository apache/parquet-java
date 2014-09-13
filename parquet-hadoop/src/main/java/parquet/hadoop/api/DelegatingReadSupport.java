package parquet.hadoop.api;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * Helps composing read supports
 *
 * @author Julien Le Dem
 *
 * @param <T>
 */
public class DelegatingReadSupport<T> extends ReadSupport<T> {

  private final ReadSupport<T> delegate;

  public DelegatingReadSupport(ReadSupport<T> delegate) {
    super();
    this.delegate = delegate;
  }

  @Override
  public ReadSupport.ReadContext init(InitContext context) {
    return delegate.init(context);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadSupport.ReadContext readContext) {
    return delegate.prepareForRead(configuration, keyValueMetaData, fileSchema, readContext);
  }

  @Override
  public String toString() {
    return this.getClass().getName() + "(" + delegate.toString() + ")";
  }
}
