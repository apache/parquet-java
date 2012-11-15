package redelm.hadoop;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnWriter;
import redelm.column.mem.MemColumn;
import redelm.column.mem.MemColumnsStore;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.schema.MessageType;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Writes records to a Redelm file
 *
 * @see RedelmOutputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class RedelmRecordWriter<T> extends
    RecordWriter<Void, T> {

  private final RedelmFileWriter w;
  private final MessageType schema;
  private final List<MetaDataBlock> extraMetaData;
  private WriteSupport<T> writeSupport;
  private int recordCount;
  private MemColumnsStore store;
  private final int blockSize;

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   */
  RedelmRecordWriter(RedelmFileWriter w, WriteSupport<T> writeSupport, MessageType schema, List<MetaDataBlock> extraMetaData, int blockSize) {
    if (writeSupport == null) {
      throw new NullPointerException("writeSupport");
    }
    this.w = w;
    this.writeSupport = writeSupport;
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.blockSize = blockSize;
    initStore();
  }

  private void initStore() {
    // TODO: parameterize this
    store = new MemColumnsStore(1024 * 1024 * 16);
    //
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema, store);
    writeSupport.initForWrite(columnIO.getRecordWriter(), schema, extraMetaData);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException,
  InterruptedException {
    flushStore();
    w.end(extraMetaData);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(Void key, T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++ recordCount;
    checkBlockSizeReached();
  }

  private void checkBlockSizeReached() throws IOException {
    if (store.memSize() > blockSize) {
      flushStore();
      initStore();
    }
  }

  private void flushStore()
      throws IOException {
    w.startBlock(recordCount);
    Collection<MemColumn> columns = store.getColumns();
    for (MemColumn column : columns) {
      ColumnDescriptor descriptor = column.getDescriptor();
      ColumnWriter columnWriter = column.getColumnWriter();
      w.startColumn(descriptor, columnWriter.getValueCount());
      w.startRepetitionLevels();
      columnWriter.writeRepetitionLevelColumn(w);
      w.startDefinitionLevels();
      columnWriter.writeDefinitionLevelColumn(w);
      w.startData();
      columnWriter.writeDataColumn(w);
      w.endColumn();
    }
    recordCount = 0;
    w.endBlock();
    store = null;
  }
}