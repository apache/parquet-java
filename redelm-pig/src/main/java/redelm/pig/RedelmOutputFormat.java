package redelm.pig;

import java.io.IOException;
import java.util.Collection;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnWriter;
import redelm.column.mem.MemColumn;
import redelm.column.mem.MemColumnsStore;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.data.Tuple;

public class RedelmOutputFormat extends FileOutputFormat<Object, Tuple> {

  private static final int THRESHOLD = 1024*1024*50;

  private TupleWriter tupleWriter;
  private MemColumnsStore store;

  private final MessageType schema;

  public RedelmOutputFormat(MessageType schema) {
    this.schema = schema;
    initStore();
  }

  private void initStore() {
    store = new MemColumnsStore(1024 * 1024 * 16);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(this.schema, store);
    tupleWriter = new TupleWriter(columnIO.getRecordWriter(), this.schema);
  }

  @Override
  public RecordWriter<Object, Tuple> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    final Path file = getDefaultWorkFile(taskAttemptContext, "");
    final Configuration conf = taskAttemptContext.getConfiguration();
    final FileSystem fs = file.getFileSystem(conf);
    final RedelmFileWriter w = new RedelmFileWriter(schema, fs.create(file, false));
    w.start();
    return new RecordWriter<Object, Tuple>() {
      private int recordCount;

      @Override
      public void close(TaskAttemptContext taskAttemptContext) throws IOException,
      InterruptedException {
        flushStore();
        w.end();
      }

      @Override
      public void write(Object key, Tuple value) throws IOException, InterruptedException {
        tupleWriter.write(value);
        ++ recordCount;
        checkBlockSizeReached();
      }

      private void checkBlockSizeReached() throws IOException {
        if (store.memSize() > THRESHOLD) {
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
        tupleWriter = null;
      }
    };
  }

}
