package parquet.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import parquet.Preconditions;
import parquet.filter2.compat.FilterCompat;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.util.ContextUtil;

/**
* An InputFormat for Parquet that is closely based on FileInputFormat.
* <p>
* This InputFormat passes Parquet objects as values. Keys are always null.
*
* @param <T> The type of value objects produced by this InputFormat.
*/
public class ParquetFileInputFormat<T> extends FileInputFormat<Void, T> {

  private final Class<? extends ReadSupport<T>> readSupportClass;

  /**
   * No-arg constructor used by the MapReduce API.
   */
  public ParquetFileInputFormat() {
    this.readSupportClass = null; // must be set in the Configuration
  }

  /**
   * Constructor for subclasses. Subclasses may use this constructor to set the
   * ReadSupport class that will be used when reading instead of requiring the
   * user to set the read support property in their configuration.
   *
   * @param readSupportClass a ReadSupport subclass
   */
  public ParquetFileInputFormat(Class<? extends ReadSupport<T>> readSupportClass) {
    this.readSupportClass = readSupportClass;
  }

  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration conf = ContextUtil.getConfiguration(taskAttemptContext);

    return new ParquetFileRecordReader<T>(
        getReadSupportInstance(conf),
        ParquetInputFormat.getFilter(conf));
  }

  private ReadSupport<T> getReadSupportInstance(Configuration conf) {
    if (readSupportClass != null) {
      return ParquetInputFormat.getReadSupportInstance(readSupportClass);
    } else {
      return ParquetInputFormat.getReadSupportInstance(conf);
    }
  }

  static class ParquetFileRecordReader<T> extends ParquetRecordReader<T> {
    public ParquetFileRecordReader(ReadSupport<T> readSupport, FilterCompat.Filter filter) {
      super(readSupport, filter);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      super.initialize(toParquetSplit(split), context);
    }

    private static ParquetInputSplit toParquetSplit(InputSplit split) throws IOException {
      Preconditions.checkArgument(split instanceof FileSplit,
          "Invalid split (not a FileSplit): " + split);
      FileSplit fileSplit = (FileSplit) split;

      return new ParquetInputSplit(fileSplit.getPath(),
          fileSplit.getStart(), fileSplit.getStart() + fileSplit.getLength(),
          fileSplit.getLength(), fileSplit.getLocations(), null);
    }
  }

}
