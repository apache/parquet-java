package parquet.avro;

import java.io.Closeable;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.CodecFactory;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.ParquetRecordWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

/**
 * Write Avro records to a Parquet file.
 */
public class AvroParquetWriter<T> implements Closeable {

  private final ParquetRecordWriter<T> writer;

  public AvroParquetWriter(Path file, Schema avroSchema) throws IOException {
    Configuration conf = new Configuration();
    MessageType schema = new AvroSchemaConverter().convert(avroSchema);

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, file);

    fileWriter.start();

    WriteSupport<T> writeSupport = (WriteSupport<T>) new AvroWriteSupport(schema,
        avroSchema); // TODO remove cast
    WriteSupport.WriteContext writeContext = writeSupport.init(conf);

    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor = codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED, 0);
    this.writer = new ParquetRecordWriter<T>
        (fileWriter, writeSupport, schema, writeContext.getExtraMetaData(), 50*1024*1024,
            1*1024*1024, compressor);
  }

  public void write(T object) throws IOException {
    try {
      writer.write(null, object);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close(null);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
