package parquet.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

abstract public class ThriftReader<T> {

  public abstract T readOneRecord(TProtocol protocol) throws TException;

}
