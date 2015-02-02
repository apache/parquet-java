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
package parquet.hadoop;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static parquet.Log.DEBUG;
import static parquet.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import parquet.Ints;
import parquet.Log;
import parquet.column.ColumnWriteStore;
import parquet.column.ParquetProperties;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.impl.ColumnWriteStoreV1;
import parquet.column.impl.ColumnWriteStoreV2;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.api.WriteSupport.FinalizedWriteContext;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.schema.MessageType;

class InternalParquetRecordWriter<T> {
  private static final Log LOG = Log.getLog(InternalParquetRecordWriter.class);

  private static final int MINIMUM_BUFFER_SIZE = 64 * 1024;
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  private final ParquetFileWriter parquetFileWriter;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private final long rowGroupSize;
  private long rowGroupSizeThreshold;
  private final int pageSize;
  private final BytesCompressor compressor;
  private final boolean validating;
  private final ParquetProperties parquetProperties;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;

  private ColumnWriteStore columnStore;
  private ColumnChunkPageWriteStore pageStore;


  /**
   * @param parquetFileWriter the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param rowGroupSize the size of a block in the file (this will be approximate)
   * @param compressor the codec used to compress
   */
  public InternalParquetRecordWriter(
      ParquetFileWriter parquetFileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long rowGroupSize,
      int pageSize,
      BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion) {
    this.parquetFileWriter = parquetFileWriter;
    this.writeSupport = checkNotNull(writeSupport, "writeSupport");
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.rowGroupSize = rowGroupSize;
    this.rowGroupSizeThreshold = rowGroupSize;
    this.pageSize = pageSize;
    this.compressor = compressor;
    this.validating = validating;
    this.parquetProperties = new ParquetProperties(dictionaryPageSize, writerVersion, enableDictionary);
    initStore();
  }

  private void initStore() {
    // we don't want this number to be too small
    // ideally we divide the block equally across the columns
    // it is unlikely all columns are going to be the same size.
    // its value is likely below Integer.MAX_VALUE (2GB), although rowGroupSize is a long type.
    // therefore this size is cast to int, since allocating byte array in under layer needs to
    // limit the array size in an int scope.
    int initialBlockBufferSize = Ints.checkedCast(max(MINIMUM_BUFFER_SIZE, rowGroupSize / schema.getColumns().size() / 5));
    pageStore = new ColumnChunkPageWriteStore(compressor, schema, initialBlockBufferSize);
    // we don't want this number to be too small either
    // ideally, slightly bigger than the page size, but not bigger than the block buffer
    int initialPageBufferSize = max(MINIMUM_BUFFER_SIZE, min(pageSize + pageSize / 10, initialBlockBufferSize));
    columnStore = parquetProperties.newColumnWriteStore(
        schema,
        pageStore,
        pageSize,
        initialPageBufferSize);
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    writeSupport.prepareForWrite(columnIO.getRecordWriter(columnStore));
  }

  public void close() throws IOException, InterruptedException {
    flushRowGroupToStore();
    FinalizedWriteContext finalWriteContext = writeSupport.finalizeWrite();
    Map<String, String> finalMetadata = new HashMap<String, String>(extraMetaData);
    finalMetadata.putAll(finalWriteContext.getExtraMetaData());
    parquetFileWriter.end(finalMetadata);
  }

  public void write(T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++ recordCount;
    checkBlockSizeReached();
  }

  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = columnStore.getBufferedSize();
      if (memSize > rowGroupSizeThreshold) {
        LOG.info(format("mem size %,d > %,d: flushing %,d records to disk.", memSize, rowGroupSizeThreshold, recordCount));
        flushRowGroupToStore();
        initStore();
        recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);
      } else {
        float recordSize = (float) memSize / recordCount;
        recordCountForNextMemCheck = min(
            max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (long)(rowGroupSizeThreshold / recordSize)) / 2), // will check halfway
            recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK // will not look more than max records ahead
            );
        if (DEBUG) LOG.debug(format("Checked mem at %,d will check again at: %,d ", recordCount, recordCountForNextMemCheck));
      }
    }
  }

  private void flushRowGroupToStore()
      throws IOException {
    LOG.info(format("Flushing mem columnStore to file. allocated memory: %,d", columnStore.getAllocatedSize()));
    if (columnStore.getAllocatedSize() > 3 * (long)rowGroupSizeThreshold) {
      LOG.warn("Too much memory used: " + columnStore.memUsageString());
    }

    if (recordCount > 0) {
      parquetFileWriter.startBlock(recordCount);
      columnStore.flush();
      pageStore.flushToFileWriter(parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();
    }

    columnStore = null;
    pageStore = null;
  }

  long getRowGroupSizeThreshold() {
    return rowGroupSizeThreshold;
  }

  void setRowGroupSizeThreshold(long rowGroupSizeThreshold) {
    this.rowGroupSizeThreshold = rowGroupSizeThreshold;
  }
}