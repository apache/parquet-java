package redelm.hadoop;

import java.io.Serializable;
import java.util.List;

import redelm.io.RecordConsumer;

abstract public class ReadSupport<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  abstract public void initForRead(List<MetaDataBlock> metaDataBlocks, String requestedSchema);

  abstract public RecordConsumer newRecordConsumer(List<T> destination);

}
