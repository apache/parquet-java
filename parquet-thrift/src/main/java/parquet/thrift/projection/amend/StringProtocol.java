package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import parquet.thrift.ParquetProtocol;

import java.nio.ByteBuffer;

class StringProtocol extends ParquetProtocol {

  private final String str;

  public StringProtocol(String str) {
    super("readString() binary");
    this.str = str;
  }

  @Override
  public String readString() throws TException {
    return str;
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return ByteBuffer.wrap("str".getBytes());
  }
}
