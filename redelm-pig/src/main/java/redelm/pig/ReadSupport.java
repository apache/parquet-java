package redelm.pig;

import java.util.List;

import redelm.io.RecordConsumer;
import redelm.schema.MessageType;

abstract public class ReadSupport<T> {

  abstract public void initForRead(RedelmInputSplit redelmInputSplit, MessageType requestedSchema);

  abstract public RecordConsumer newRecordConsumer(List<T> destination);

}
