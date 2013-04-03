package parquet.column.values;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.dictionary.DictionaryValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.io.api.Binary;

public class BinaryEncodingPickerValuesWriter extends ValuesWriter {

  private final int maxDictionaryByteSize;
  private DictionaryValuesWriter dictionaryValuesWriter = new DictionaryValuesWriter();
  private PlainValuesWriter plainValuesWriter;
  private boolean dictionaryUsed = false;
  private boolean dictionaryTooBig = false;

  public BinaryEncodingPickerValuesWriter(int maxDictionaryByteSize, int initialSize) {
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.plainValuesWriter = new PlainValuesWriter(initialSize);
  }

  @Override
  public void writeBytes(Binary v) {
    if (!dictionaryTooBig) {
      dictionaryValuesWriter.writeBytes(v);
      if (dictionaryValuesWriter.getDictionaryByteSize() > maxDictionaryByteSize) {
        dictionaryTooBig = true;
        if (!dictionaryUsed) {
          dictionaryValuesWriter = null;
        }
      }
    }
    plainValuesWriter.writeBytes(v);
  }

  @Override
  public int getDictionarySize() {
    if (dictionaryUsed) {
      return dictionaryValuesWriter.getDictionarySize();
    }
    return plainValuesWriter.getDictionarySize();
  }

  @Override
  public BytesInput getDictionaryBytes() {
    if (dictionaryUsed) {
      return dictionaryValuesWriter.getDictionaryBytes();
    }
    return plainValuesWriter.getDictionaryBytes();
  }

  @Override
  public Encoding getDictionaryEncoding() {
    if (dictionaryUsed) {
      return dictionaryValuesWriter.getEncoding();
    }
    return plainValuesWriter.getEncoding();
  }

  @Override
  public BytesInput getBytes() {
    if (!dictionaryTooBig) {
      dictionaryUsed = true;
      return dictionaryValuesWriter.getBytes();
    }
    return plainValuesWriter.getBytes();
  }

  @Override
  public Encoding getEncoding() {
    if (!dictionaryTooBig) {
      return dictionaryValuesWriter.getEncoding();
    }
    return plainValuesWriter.getEncoding();
  }

  @Override
  public void reset() {
    if (dictionaryValuesWriter != null) {
      dictionaryValuesWriter.reset();
    }
    plainValuesWriter.reset();
  }

  @Override
  public long getBufferedSize() {
    return (dictionaryValuesWriter == null ? 0 : dictionaryValuesWriter.getBufferedSize()) + plainValuesWriter.getBufferedSize();
  }

  @Override
  public long getAllocatedSize() {
    return (dictionaryValuesWriter == null ? 0 : dictionaryValuesWriter.getAllocatedSize()) + plainValuesWriter.getAllocatedSize();
  }

}
