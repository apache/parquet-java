package parquet.scrooge;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec;

import parquet.schema.MessageType;
import parquet.thrift.ThriftReader;
import parquet.thrift.ThriftRecordConverter;
import parquet.thrift.struct.ThriftType.StructType;

public class ScroogeRecordConverter<T extends ThriftStruct> extends ThriftRecordConverter<T> {


  public ScroogeRecordConverter(final Class<T> thriftClass, MessageType parquetSchema, StructType thriftType) {
    super(new ThriftReader<T>() {
      @SuppressWarnings("unchecked")
      ThriftStructCodec<T> codec = (ThriftStructCodec<T>) getCodec(thriftClass);
      @Override
      public T readOneRecord(TProtocol protocol) throws TException {
          return codec.decode(protocol);
      }
    }, thriftClass.getSimpleName(), parquetSchema, thriftType);
  }

  private static ThriftStructCodec<?> getCodec(Class<?> klass) {
    Class<?> companionClass;
    try {
      companionClass = Class.forName(klass.getName() + "$");
      Object companionObject = companionClass.getField("MODULE$").get(null);
      return (ThriftStructCodec<?>) companionObject;
    } catch (Throwable t) {
      throw new RuntimeException("Unable to create ThriftStructCodec", t);
    }
  }
}
