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
package org.apache.parquet.arrow.writer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.util.AutoCloseables;

/**
 * Writes Arrow {@link VectorSchemaRoot} batches to Parquet files using page-level operations.
 *
 * <p>Unlike the standard {@code ParquetWriter<T>} which processes one record at a time through
 * a RecordConsumer, this writer operates at the column/page level. For non-null fixed-width
 * PLAIN-encoded columns, Arrow data buffers are wrapped directly as Parquet pages with zero
 * per-value overhead.
 *
 * <p>Usage:
 * <pre>{@code
 * MessageType schema = new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
 * try (ArrowParquetWriter writer = new ArrowParquetWriter(outputFile, schema)) {
 *   writer.writeBatch(batch);
 * }
 * }</pre>
 *
 * <p>This writer is NOT thread-safe.
 */
public class ArrowParquetWriter implements Closeable {

  private static final long DEFAULT_ROW_GROUP_SIZE = 128 * 1024 * 1024L;

  /** Target page size. Pages larger than this reduce column-index predicate pushdown granularity. */
  private static final int TARGET_PAGE_SIZE_BYTES = 1024 * 1024;

  /** Default byte estimate for variable-width columns when computing rows-per-page. */
  private static final int VAR_WIDTH_ESTIMATE_BYTES = 32;

  private final ParquetFileWriter fileWriter;
  private final MessageType schema;
  private final ParquetProperties props;
  private final Function<ColumnDescriptor, BytesInputCompressor> compressorProvider;
  private final CompressionCodecFactory codecFactory;
  private final long rowGroupSizeThreshold;

  private ArrowColumnWriter[] columnWriters;
  private ColumnChunkPageWriteStore pageStore;
  private long rowGroupRowCount = 0;
  private int rowGroupOrdinal = 0;
  private boolean closed = false;

  /**
   * Creates a writer with no compression (no Hadoop dependency at runtime).
   *
   * @param file the output file
   * @param schema the Parquet schema
   * @throws IOException if an I/O error occurs
   */
  public ArrowParquetWriter(OutputFile file, MessageType schema) throws IOException {
    this(file, schema, CompressionCodecName.UNCOMPRESSED, null,
        DEFAULT_ROW_GROUP_SIZE, ParquetProperties.builder().build());
  }

  /**
   * Creates a writer with explicit configuration.
   *
   * @param file the output file
   * @param schema the Parquet schema
   * @param codec the compression codec
   * @param codecFactory factory for compressors (required if codec != UNCOMPRESSED)
   * @param rowGroupSize target row group size in bytes
   * @param props Parquet properties
   * @throws IOException if an I/O error occurs
   */
  public ArrowParquetWriter(
      OutputFile file,
      MessageType schema,
      CompressionCodecName codec,
      CompressionCodecFactory codecFactory,
      long rowGroupSize,
      ParquetProperties props) throws IOException {
    this.schema = Objects.requireNonNull(schema, "schema cannot be null");
    this.props = Objects.requireNonNull(props, "props cannot be null");
    this.rowGroupSizeThreshold = rowGroupSize;
    this.codecFactory = codecFactory;

    if (codec == CompressionCodecName.UNCOMPRESSED) {
      this.compressorProvider = column -> UNCOMPRESSED;
    } else {
      Objects.requireNonNull(codecFactory,
          "codecFactory required for compression codec: " + codec);
      this.compressorProvider = column -> codecFactory.getCompressor(codec);
    }

    this.fileWriter = new ParquetFileWriter(
        file, schema, ParquetFileWriter.Mode.CREATE, rowGroupSize, 0);
    this.fileWriter.start();

    initRowGroup();
  }

  /**
   * Convenience factory that derives the Parquet schema from an Arrow schema.
   *
   * @param file the output file
   * @param arrowSchema the Arrow schema to convert
   * @return a new writer configured for UNCOMPRESSED output
   * @throws IOException if an I/O error occurs
   */
  public static ArrowParquetWriter fromArrowSchema(OutputFile file, Schema arrowSchema)
      throws IOException {
    MessageType parquetSchema =
        new SchemaConverter().fromArrow(arrowSchema).getParquetSchema();
    return new ArrowParquetWriter(file, parquetSchema);
  }

  /**
   * Writes all rows in the batch to the file. Each column is written using its optimal
   * strategy (zero-copy when possible, bulk-copy for nullable, etc.).
   * Large batches are split into page-sized chunks.
   *
   * @param batch the Arrow batch to write
   * @throws IOException if an I/O error occurs
   * @throws IllegalArgumentException if the batch field count does not match the schema
   */
  public void writeBatch(VectorSchemaRoot batch) throws IOException {
    List<FieldVector> vectors = batch.getFieldVectors();
    int fieldCount = vectors.size();
    int rowCount = batch.getRowCount();

    if (fieldCount != schema.getFieldCount()) {
      throw new IllegalArgumentException(
          "Batch has " + fieldCount + " columns but schema has " + schema.getFieldCount());
    }

    if (rowCount == 0) {
      return;
    }

    // Estimate rows per page based on schema width
    int estimatedRowBytes = 0;
    for (ColumnDescriptor col : schema.getColumns()) {
      int typeLen = col.getPrimitiveType().getTypeLength();
      estimatedRowBytes += typeLen > 0 ? typeLen : VAR_WIDTH_ESTIMATE_BYTES;
    }
    int rowsPerPage = Math.max(1, TARGET_PAGE_SIZE_BYTES / Math.max(estimatedRowBytes, 1));

    // Write in page-sized chunks
    int offset = 0;
    while (offset < rowCount) {
      int chunkSize = Math.min(rowsPerPage, rowCount - offset);
      for (int col = 0; col < fieldCount; col++) {
        columnWriters[col].write(vectors.get(col), offset, chunkSize);
      }
      rowGroupRowCount += chunkSize;
      offset += chunkSize;

      // Check row group threshold after each page
      if (shouldFlushRowGroup()) {
        flushRowGroup();
        initRowGroup();
      }
    }
  }

  /**
   * Returns file metadata. Only valid after close.
   */
  public ParquetMetadata getFooter() {
    return fileWriter.getFooter();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closed = true;
      try {
        if (rowGroupRowCount > 0) {
          flushRowGroup();
        }
        fileWriter.end(Collections.emptyMap());
      } finally {
        AutoCloseables.uncheckedClose(pageStore, fileWriter);
        if (codecFactory != null) {
          codecFactory.release();
        }
      }
    }
  }

  private void initRowGroup() {
    pageStore = ColumnChunkPageWriteStore.builder()
        .withCompressorProvider(compressorProvider)
        .withSchema(schema)
        .withAllocator(props.getAllocator())
        .withColumnIndexTruncateLength(props.getColumnIndexTruncateLength())
        .withPageWriteChecksumEnabled(props.getPageWriteChecksumEnabled())
        .withRowGroupOrdinal(rowGroupOrdinal)
        .build();

    int fieldCount = schema.getFieldCount();
    columnWriters = new ArrowColumnWriter[fieldCount];
    List<ColumnDescriptor> columns = schema.getColumns();

    for (int i = 0; i < fieldCount; i++) {
      PageWriter pageWriter = pageStore.getPageWriter(columns.get(i));
      columnWriters[i] = ArrowColumnWriterFactory.create(schema, i, pageWriter);
    }

    rowGroupRowCount = 0;
  }

  private boolean shouldFlushRowGroup() {
    // Query actual buffered size from page writers
    long totalBuffered = 0;
    List<ColumnDescriptor> columns = schema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      totalBuffered += pageStore.getPageWriter(columns.get(i)).getMemSize();
    }
    return totalBuffered >= rowGroupSizeThreshold;
  }

  private void flushRowGroup() throws IOException {
    try {
      rowGroupOrdinal++;
      fileWriter.startBlock(rowGroupRowCount);
      pageStore.flushToFileWriter(fileWriter);
      fileWriter.endBlock();
    } finally {
      AutoCloseables.uncheckedClose(pageStore);
      pageStore = null;
      columnWriters = null;
    }
  }

  private static final BytesInputCompressor UNCOMPRESSED = new BytesInputCompressor() {
    @Override
    public BytesInput compress(BytesInput bytes) {
      return bytes;
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.UNCOMPRESSED;
    }

    @Override
    public void release() {}
  };
}
