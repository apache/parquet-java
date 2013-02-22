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

import java.io.IOException;
import java.util.Map;


import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import parquet.Log;
import parquet.column.mem.MemColumnWriteStore;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.schema.MessageType;

/**
 * Writes records to a Parquet file
 *
 * @see ParquetOutputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class ParquetRecordWriter<T> extends RecordWriter<Void, T> {
  private static final Log LOG = Log.getLog(ParquetRecordWriter.class);

  private final ParquetFileWriter w;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private final int blockSize;
  private final int pageSize;
  private final BytesCompressor compressor;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = 100;

  private MemColumnWriteStore store;
  private ColumnChunkPageWriteStore pageStore;


  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param codec the codec used to compress
   */
  ParquetRecordWriter(ParquetFileWriter w, WriteSupport<T> writeSupport, MessageType schema,  Map<String, String> extraMetaData, int blockSize, int pageSize, BytesCompressor compressor) {
    if (writeSupport == null) {
      throw new NullPointerException("writeSupport");
    }
    this.w = w;
    this.writeSupport = writeSupport;
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.blockSize = blockSize;
    this.pageSize = pageSize;
    this.compressor = compressor;
    initStore();
  }

  private void initStore() {
    pageStore = new ColumnChunkPageWriteStore(compressor, schema);
    store = new MemColumnWriteStore(pageStore, pageSize);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    writeSupport.prepareForWrite(columnIO.getRecordWriter(store));
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
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = store.memSize();
      float recordSize = (float) memSize / recordCount;
      recordCountForNextMemCheck = (long)(blockSize * 0.9 / recordSize); // -10% so that we don't miss it.
      if (recordCountForNextMemCheck - recordCount > 1000) {
        LOG.info("Checked mem at " + recordCount + " will check again at: " + recordCountForNextMemCheck);
      }
      if (memSize > blockSize) {
        LOG.info("mem size " + memSize + " > " + blockSize + ": flushing " + recordCount + " records to disk.");
        flushStore();
        initStore();
      }
    }
  }

  private void flushStore()
      throws IOException {
    LOG.info("Flushing mem store to file. allocated memory: " + store.allocatedSize());
    w.startBlock(recordCount);
    store.flush();
    pageStore.flushToFileWriter(w);
    recordCount = 0;
    w.endBlock();
    store = null;
    pageStore = null;
  }
}