package redelm.hadoop;

import java.util.List;

import redelm.io.RecordConsumer;
import redelm.schema.MessageType;

/**
 * Abstraction to use with {@link RedelmOutputFormat} to convert incoming records
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the incoming records
 */
abstract public class WriteSupport<T> {

  /**
   * @param recordConsumer the recordConsumer to write to
   * @param schema the schema of the incoming records
   * @param extraMetaData extra meta data being written to the footer of the file
   */
  public abstract void initForWrite(RecordConsumer recordConsumer, MessageType schema, List<MetaDataBlock> extraMetaData);

  /**
   *
   * @param record one record to write
   */
  public abstract void write(T record);

}
