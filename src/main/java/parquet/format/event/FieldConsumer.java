package parquet.format.event;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

/**
 * To receive Thrift field events
 *
 * @author Julien Le Dem
 *
 */
public interface FieldConsumer {

  /**
   * called by the EventBasedThriftReader when reading a field from a Struct
   * @param protocol the underlying protocol
   * @param eventBasedThriftReader the reader to delegate to further calls.
   * @param id the id of the field
   * @param type the type of the field
   * @return the typed consumer to pass the value to
   * @throws TException
   */
  public void consumeField(TProtocol protocol, EventBasedThriftReader eventBasedThriftReader, short id, byte type) throws TException;

}