/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.avro;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;

/**
 * Write Avro records to a Parquet file.
 */
public class AvroParquetWriter<T extends IndexedRecord> extends ParquetWriter<T> {

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file
   * @param avroSchema
   * @param compressionCodecName
   * @param blockSize
   * @param pageSize
   * @throws IOException
   */
  public AvroParquetWriter(Path file, Schema avroSchema,
      CompressionCodecName compressionCodecName, int blockSize,
      int pageSize) throws IOException {
    super(file, AvroParquetWriter.<T>writeSupport(avroSchema),
	      compressionCodecName, blockSize, pageSize);
  }

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize the block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @throws IOException
   */
  public AvroParquetWriter(Path file, Schema avroSchema,
                           CompressionCodecName compressionCodecName, int blockSize,
                           int pageSize, boolean enableDictionary) throws IOException {
    super(file, AvroParquetWriter.<T>writeSupport(avroSchema),
        compressionCodecName, blockSize, pageSize, enableDictionary,
        DEFAULT_IS_VALIDATING_ENABLED);
  }

  /** Create a new {@link AvroParquetWriter}. The default block size is 50 MB.The default
   *  page size is 1 MB.  Default compression is no compression. (Inherited from {@link ParquetWriter})
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @throws IOException
   */
  public AvroParquetWriter(Path file, Schema avroSchema) throws IOException {
    this(file, avroSchema, CompressionCodecName.UNCOMPRESSED,
	  DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  /** Create a new {@link AvroParquetWriter}.
   *
   * @param file The file name to write to.
   * @param avroSchema The schema to write with.
   * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
   * @param blockSize the block size threshold.
   * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
   * @param enableDictionary Whether to use a dictionary to compress columns.
   * @param conf The Configuration to use.
   * @throws IOException
   */
  public AvroParquetWriter(Path file, Schema avroSchema,
                           CompressionCodecName compressionCodecName,
                           int blockSize, int pageSize, boolean enableDictionary,
                           Configuration conf) throws IOException {
    super(file, AvroParquetWriter.<T>writeSupport(avroSchema),
        compressionCodecName, blockSize, pageSize, pageSize, enableDictionary,
        DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION, conf);
  }

  @SuppressWarnings("unchecked")
  private static <T> WriteSupport<T> writeSupport(Schema avroSchema) {
    return (WriteSupport<T>) new AvroWriteSupport(
        new AvroSchemaConverter().convert(avroSchema), avroSchema);
  }
}
