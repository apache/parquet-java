package redelm.hadoop;

import redelm.io.RecordConsumer;
import redelm.schema.MessageType;

abstract public class WriteSupport<T> {

  public abstract void initForWrite(RecordConsumer recordConsumer, MessageType schema);

  public abstract void write(T record);

}
