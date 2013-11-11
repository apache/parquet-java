package parquet.column.values.deltalengthbytearray;

import java.io.IOException;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.bytes.LittleEndianDataOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.io.ParquetEncodingException;
import parquet.io.api.Binary;

public class DeltaLengthByteArrayValuesWriter extends ValuesWriter {

  private static final Log LOG = Log.getLog(DeltaLengthByteArrayValuesWriter.class);

  private ValuesWriter lengthWriter;
  private CapacityByteArrayOutputStream arrayOut;
  private LittleEndianDataOutputStream out;

  public DeltaLengthByteArrayValuesWriter(int initialSize) {
    arrayOut = new CapacityByteArrayOutputStream(initialSize);
    out = new LittleEndianDataOutputStream(arrayOut);
    lengthWriter = new PlainValuesWriter(initialSize);
  }

  @Override
  public void writeBytes(Binary v) {
    try {
      lengthWriter.writeInteger(v.length());
      out.write(v.getBytes());
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write bytes", e);
    }
  }

  @Override
  public long getBufferedSize() {
    return lengthWriter.getBufferedSize() + arrayOut.size();
  }

  @Override
  public BytesInput getBytes() {
    try {
      out.flush();
    } catch (IOException e) {
      throw new ParquetEncodingException("could not write page", e);
    }
    if (Log.DEBUG) LOG.debug("writing a buffer of size " + arrayOut.size());
    return BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(arrayOut));
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.DELTA_LENGTH_BYTE_ARRAY;
  }

  @Override
  public void reset() {
    lengthWriter.reset();
    arrayOut.reset();
  }

  @Override
  public long getAllocatedSize() {
    return lengthWriter.getAllocatedSize() + arrayOut.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return arrayOut.memUsageString(lengthWriter.memUsageString(prefix) + " DELTA_LENGTH_BYTE_ARRAY"); 
  }

}
