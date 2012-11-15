package redelm.hadoop;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.mapred.InputFormat;

import redelm.io.RecordConsumer;

/**
 * Abstraction used by the {@link RedelmInputFormat} to materialize records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
abstract public class ReadSupport<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  /**
   * called in {@link InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)} when the file footer is read
   * @param metaDataBlocks metadata blocks from the footer
   * @param requestedSchema the schema requested by the user
   */
  abstract public void initForRead(List<MetaDataBlock> metaDataBlocks, String requestedSchema);

  /**
   * called by the record reader in the backend.
   * the returned RecordConsumer will materialize the records and add them to the destination
   * @param destination where to write the records
   * @return the recordConsumer that will receive the events
   */
  abstract public RecordConsumer newRecordConsumer(List<T> destination);

}
