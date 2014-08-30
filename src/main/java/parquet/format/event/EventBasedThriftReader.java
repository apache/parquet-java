package parquet.format.event;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;

import parquet.format.event.TypedConsumer.ListConsumer;
import parquet.format.event.TypedConsumer.MapConsumer;
import parquet.format.event.TypedConsumer.SetConsumer;

/**
 * Event based reader for Thrift
 *
 * @author Julien Le Dem
 *
 */
public final class EventBasedThriftReader {

  private final TProtocol protocol;

  /**
   * @param protocol the protocol to read from
   */
  public EventBasedThriftReader(TProtocol protocol) {
    this.protocol = protocol;
  }

  /**
   * reads a Struct from the underlying protocol and passes the field events to the FieldConsumer
   * @param c the field consumer
   * @throws TException
   */
  public void readStruct(FieldConsumer c) throws TException {
    protocol.readStructBegin();
    readStructContent(c);
    protocol.readStructEnd();
  }

  /**
   * reads the content of a struct (fields) from the underlying protocol and passes the events to c
   * @param c the field consumer
   * @throws TException
   */
  public void readStructContent(FieldConsumer c) throws TException {
    TField field;
    while (true) {
      field = protocol.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      c.consumeField(protocol, this, field.id, field.type);
    }
  }

  /**
   * reads the set content (elements) from the underlying protocol and passes the events to the set event consumer
   * @param eventConsumer the consumer
   * @param tSet the set descriptor
   * @throws TException
   */
  public void readSetContent(SetConsumer eventConsumer, TSet tSet)
      throws TException {
    for (int i = 0; i < tSet.size; i++) {
      eventConsumer.consumeElement(protocol, this, tSet.elemType);
    }
  }

  /**
   * reads the map content (key values) from the underlying protocol and passes the events to the map event consumer
   * @param eventConsumer the consumer
   * @param tMap the map descriptor
   * @throws TException
   */
  public void readMapContent(MapConsumer eventConsumer, TMap tMap)
      throws TException {
    for (int i = 0; i < tMap.size; i++) {
      eventConsumer.consumeEntry(protocol, this, tMap.keyType, tMap.valueType);
    }
  }

  /**
   * reads a key-value pair
   * @param keyType the type of the key
   * @param keyConsumer the consumer for the key
   * @param valueType the type of the value
   * @param valueConsumer the consumer for the value
   * @throws TException
   */
  public void readMapEntry(byte keyType, TypedConsumer keyConsumer, byte valueType, TypedConsumer valueConsumer)
      throws TException {
    keyConsumer.read(protocol, this, keyType);
    valueConsumer.read(protocol, this, valueType);
  }

  /**
   * reads the list content (elements) from the underlying protocol and passes the events to the list event consumer
   * @param eventConsumer the consumer
   * @param tList the list descriptor
   * @throws TException
   */
  public void readListContent(ListConsumer eventConsumer, TList tList)
      throws TException {
    for (int i = 0; i < tList.size; i++) {
      eventConsumer.consumeElement(protocol, this, tList.elemType);
    }
  }
}