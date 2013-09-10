package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import parquet.thrift.ParquetProtocol;

import java.nio.ByteBuffer;

/**
* Created with IntelliJ IDEA.
* User: tdeng
* Date: 9/10/13
* Time: 10:33 AM
* To change this template use File | Settings | File Templates.
*/
class StringProtocol extends ParquetProtocol {

  private final String str;

  public StringProtocol(String str) {
    super("readString() binary");
    this.str = str;
  }

  @Override
  public String readString() throws TException {
    return "str";
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return ByteBuffer.wrap("str".getBytes());
  }
}
