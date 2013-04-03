package parquet.column.values.dictionary;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

public class DictionaryValuesWriter extends ValuesWriter {

  private int dictionaryByteSize = 0;
  private Map<Binary, Integer> dict = new LinkedHashMap<Binary, Integer>();
  private CapacityByteArrayOutputStream baos = new CapacityByteArrayOutputStream(32 * 1024); // TODO: initial size
  private LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(baos);

  @Override
  public void writeBytes(Binary v) {
    Integer id = dict.get(v);
    if (id == null) {
      id = dict.size();
      dict.put(v, id);
      // length as int + actual bytes
      dictionaryByteSize += 4 + v.length();
    }
    try {
      out.writeInt(id);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getBufferedSize() {
    return baos.size() + dictionaryByteSize;
  }

  @Override
  public BytesInput getBytes() {
    return BytesInput.from(baos);
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.PLAIN_DICTIONARY;
  }

  @Override
  public void reset() {
    baos.reset();
  }

  @Override
  public long getAllocatedSize() {
    return baos.getCapacity() + dictionaryByteSize;
  }

  @Override
  public BytesInput getDictionaryBytes() {
    try {
      CapacityByteArrayOutputStream dictBuf = new CapacityByteArrayOutputStream(dictionaryByteSize);
      LittleEndianDataOutputStream dictOut = new LittleEndianDataOutputStream(dictBuf);
      for (Binary entry : dict.keySet()) {
        dictOut.writeInt(entry.length());
        entry.writeTo(dictOut);
      }
      return BytesInput.from(dictBuf);
    } catch (IOException e) {
      throw new ParquetEncodingException("Could not generate dictionary Page", e);
    }
  }

  @Override
  public int getDictionarySize() {
    return dict.size();
  }
}
