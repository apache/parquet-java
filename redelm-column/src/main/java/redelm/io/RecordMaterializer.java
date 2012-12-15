package redelm.io;

/**
 *
 * materializes records coming from the assembly algorithm
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized tuple
 */
abstract public class RecordMaterializer<T> extends RecordConsumer {

  /**
   * called after a call to endMessage()
   * @return the materialized record
   */
  abstract public T getCurrentRecord();

}
