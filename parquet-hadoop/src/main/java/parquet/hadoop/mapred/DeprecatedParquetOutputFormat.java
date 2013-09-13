package parquet.hadoop.mapred;

import static parquet.Log.INFO;
import parquet.Log;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetRecordWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

@SuppressWarnings("deprecation")
public class DeprecatedParquetOutputFormat<V> extends org.apache.hadoop.mapred.FileOutputFormat<Void, V> {
  private static final Log LOG = Log.getLog(ParquetOutputFormat.class);

  protected ParquetOutputFormat<V> realOutputFormat = new ParquetOutputFormat<V>();

  @Override
  public RecordWriter<Void, V> getRecordWriter(FileSystem fs,
      JobConf conf, String name, Progressable progress) throws IOException {
    return new RecordWriterWrapper<V>(realOutputFormat, fs, conf, name, progress);
  }

  private static CompressionCodecName getCodec(JobConf conf) {
    CompressionCodecName codec;

    if (ParquetOutputFormat.isCompressionSet(conf)) { // explicit parquet config
      codec = ParquetOutputFormat.getCompression(conf);
    } else if (getCompressOutput(conf)) { // from hadoop config
      // find the right codec
      Class<?> codecClass = getOutputCompressorClass(conf, DefaultCodec.class);
      if (INFO) LOG.info("Compression set through hadoop codec: " + codecClass.getName());
      codec = CompressionCodecName.fromCompressionCodec(codecClass);
    } else {
      if (INFO) LOG.info("Compression set to false");
      codec = CompressionCodecName.UNCOMPRESSED;
    }

    if (INFO) LOG.info("Compression: " + codec.name());
    return codec;
  }

  private static Path getDefaultWorkFile(JobConf conf, String name, String extension) {
    String file = getUniqueName(conf, name) + extension;
    return new Path(getWorkOutputPath(conf), file);
  }

  private static class RecordWriterWrapper<V> implements RecordWriter<Void, V> {

    private ParquetRecordWriter<V> realWriter;

    public RecordWriterWrapper(ParquetOutputFormat<V> realOutputFormat,
        FileSystem fs, JobConf conf, String name, Progressable progress) throws IOException {

      CompressionCodecName codec = getCodec(conf);
      String extension = codec.getExtension() + ".parquet";
      Path file = getDefaultWorkFile(conf, name, extension);

      try {
        realWriter = (ParquetRecordWriter<V>) realOutputFormat.getRecordWriter(conf, file, codec);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      try {
        realWriter.close(null);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void write(Void key, V value) throws IOException {
      try {
        realWriter.write(key, value);
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }
  }
}
