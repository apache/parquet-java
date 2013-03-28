package parquet.column.values.dictionary;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.io.api.Binary;

public class DictionaryColumnWriter extends ValuesWriter {

  private Map<Binary, Integer> dict = new HashMap<Binary, Integer>();
  private CapacityByteArrayOutputStream baos = new CapacityByteArrayOutputStream(32 * 1024);
  private LittleEndianDataOutputStream out = new LittleEndianDataOutputStream(baos);

  @Override
  public void writeBytes(Binary v) {
   Integer id = dict.get(v);
   if (id == null) {
     dict.put(v, dict.size());
   }
   try {
    out.writeInt(id);
  } catch (IOException e) {
    throw new RuntimeException(e);
  }
  }

  @Override
  public long getBufferedSize() {
    return baos.size();
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
    return baos.getCapacity();
  }

}
