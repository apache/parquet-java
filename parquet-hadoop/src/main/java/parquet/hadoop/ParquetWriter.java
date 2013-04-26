/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {

  private final ParquetRecordWriter<T> writer;

  public ParquetWriter(Path file, WriteSupport<T> writeSupport) throws IOException {
    Configuration conf = new Configuration();

    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, file);
    fileWriter.start();

    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor =
        codecFactory.getCompressor(CompressionCodecName.UNCOMPRESSED, 0);
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
