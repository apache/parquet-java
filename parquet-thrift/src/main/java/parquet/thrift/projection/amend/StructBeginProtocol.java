package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TStruct;
import parquet.thrift.ParquetProtocol;

/**
 * Created with IntelliJ IDEA.
 * User: tdeng
 * Date: 9/10/13
 * Time: 10:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class StructBeginProtocol extends ParquetProtocol {
  private final String structName;

  public StructBeginProtocol(String structName) {
    super("readStructBegin()");
    this.structName=structName;
  }
  @Override
  public TStruct readStructBegin() throws TException {
    return new TStruct(structName);
  }
}
