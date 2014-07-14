package parquet.filter2.recordlevel;

import parquet.Preconditions;
import parquet.column.Dictionary;
import parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate.ValueInspector;
import parquet.io.api.Binary;
import parquet.io.api.PrimitiveConverter;

/**
 * see {@link FilteringRecordMaterializer}
 *
 * This pass-through proxy for a delegate {@link PrimitiveConverter} also
 * updates the {@link ValueInspector}s of a {@link IncrementallyUpdatedFilterPredicate}
 */
public class FilteringPrimitiveConverter extends PrimitiveConverter {
  private final PrimitiveConverter delegate;
  private final ValueInspector[] valueInspectors;

  public FilteringPrimitiveConverter(PrimitiveConverter delegate, ValueInspector[] valueInspectors) {
    this.delegate = Preconditions.checkNotNull(delegate, "delegate");
    this.valueInspectors = Preconditions.checkNotNull(valueInspectors, "valueInspectors");
  }

  // TODO(alexlevenson): this essentially turns off dictionary support
  // TODO(alexlevenson): even if the underlying delegate supports it
  // TODO(alexlevenson): we should support it here
  @Override
  public boolean hasDictionarySupport() {
    return false;
  }

  @Override
  public void setDictionary(Dictionary dictionary) {
    throw new UnsupportedOperationException("FilteringPrimitiveConverter doesn't have dictionary support");
  }

  @Override
  public void addValueFromDictionary(int dictionaryId) {
    throw new UnsupportedOperationException("FilteringPrimitiveConverter doesn't have dictionary support");
  }

  @Override
  public void addBinary(Binary value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addBinary(value);
  }

  @Override
  public void addBoolean(boolean value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addBoolean(value);
  }

  @Override
  public void addDouble(double value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addDouble(value);
  }

  @Override
  public void addFloat(float value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addFloat(value);
  }

  @Override
  public void addInt(int value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addInt(value);
  }

  @Override
  public void addLong(long value) {
    for (ValueInspector valueInspector : valueInspectors) {
      valueInspector.update(value);
    }
    delegate.addLong(value);
  }
}
