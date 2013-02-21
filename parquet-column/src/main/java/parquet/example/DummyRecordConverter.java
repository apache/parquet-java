package parquet.example;

import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;

/**
 * Dummy implementation for perf tests
 *
 * @author Julien Le Dem
 *
 */
public final class DummyRecordConverter extends
    RecordConverter<Object> {
  Object a;

  public Object getCurrentRecord() {
    return a;
  }

  public GroupConverter getGroupConverter(int fieldIndex) {
    return new DummyRecordConverter();
  }

  public PrimitiveConverter getPrimitiveConverter(int fieldIndex) {
    return new PrimitiveConverter() {
      public void addBinary(byte[] value) {
        a = value;
      }
      public void addBoolean(boolean value) {
        a = value;
      }
      public void addDouble(double value) {
        a = value;
      }
      public void addFloat(float value) {
        a = value;
      }
      public void addInt(int value) {
        a = value;
      }
      public void addLong(long value) {
        a = value;
      }
    };
  }

  public void start() {
    a = "start()";
  }

  public void end() {
    a = "end()";
  }
}