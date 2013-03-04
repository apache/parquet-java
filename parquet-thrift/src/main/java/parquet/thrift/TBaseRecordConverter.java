package parquet.thrift;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import parquet.schema.MessageType;
import parquet.thrift.struct.ThriftType.StructType;

public class TBaseRecordConverter<T extends TBase<?,?>> extends ThriftRecordConverter<T> {

  public TBaseRecordConverter(final Class<T> thriftClass, MessageType parquetSchema, StructType thriftType) {
    super(new ThriftReader<T>() {
      @Override
      public T readOneRecord(TProtocol protocol) throws TException {
        try {
          T thriftObject = thriftClass.newInstance();
          thriftObject.read(protocol);
          return thriftObject;
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }, thriftClass.getSimpleName(), parquetSchema, thriftType);
  }

}
