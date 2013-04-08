package parquet.avro;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;

/**
 * Read Avro records from a Parquet file.
 */
public class AvroParquetReader<T> implements Closeable {

  private ParquetRecordReader<T> reader;

  public AvroParquetReader(Path file) throws IOException {
    Configuration conf = new Configuration();
    reader = new ParquetRecordReader<T>((ReadSupport<T>) new
        AvroReadSupport()); // TODO: remove cast

    FileSystem fs = FileSystem.get(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file));
    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses);
    Footer footer = footers.get(0); // TODO: check only one

    List<BlockMetaData> blocks = footer.getParquetMetadata().getBlocks();
    FileMetaData fileMetaData = footer.getParquetMetadata().getFileMetaData();
    String schema = fileMetaData.getSchema().toString();
    Map<String, String> extraMetadata = fileMetaData.getKeyValueMetaData();
    ParquetInputSplit inputSplit = new ParquetInputSplit(file, 0, 0, null, blocks,
        schema, schema, extraMetadata);
    TaskAttemptContext taskAttemptContext = new TaskAttemptContext(conf, new TaskAttemptID("test", 1, true, 1, 1));
    try {
      reader.initialize(inputSplit, taskAttemptContext);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public T read() throws IOException {
    try {
      return reader.nextKeyValue() ? reader.getCurrentValue() : null;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
