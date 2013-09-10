package parquet.thrift.projection.amend;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import parquet.thrift.ParquetProtocol;
import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftTypeID;

/**
* Created with IntelliJ IDEA.
* User: tdeng
* Date: 9/10/13
* Time: 10:27 AM
* To change this template use File | Settings | File Templates.
*/
class ReadFieldBeginProtocol extends ParquetProtocol {
  private final ThriftField field;
  private final byte thriftType;

  public ReadFieldBeginProtocol(ThriftField missingField) {
    super("readFieldBegin()");
    this.field = missingField;
    this.thriftType =
            missingField.getType().getType() == ThriftTypeID.ENUM ?
                    ThriftTypeID.I32.getThriftType() : // enums are serialized as I32
                    missingField.getType().getType().getThriftType();
  }

  @Override
  public TField readFieldBegin() throws TException {
    return new TField(field.getName(), thriftType, field.getFieldId());
  }
}
