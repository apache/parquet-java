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
package redelm.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import redelm.bytes.BytesInput;
import redelm.column.ColumnDescriptor;
import redelm.column.ColumnWriter;
import redelm.column.mem.MemColumn;
import redelm.column.mem.MemColumnsStore;
import redelm.column.mem.MemPageStore;
import redelm.column.mem.Page;
import redelm.column.mem.PageReader;
import redelm.column.mem.PageStore;
import redelm.column.mem.PageWriter;
import redelm.hadoop.metadata.CompressionCodecName;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.schema.MessageType;

/**
 * Writes records to a Redelm file
 *
 * @see RedelmOutputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class RedelmRecordWriter<T> extends
    RecordWriter<Void, T> {

  private final RedelmFileWriter w;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private WriteSupport<T> writeSupport;
  private int recordCount;
  private MemColumnsStore store;
  private final int blockSize;
  private final CompressionCodecName codec;

  private MemPageStore pageStore;

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   * @param codec the codec used to compress
   */
  RedelmRecordWriter(RedelmFileWriter w, WriteSupport<T> writeSupport, MessageType schema,  Map<String, String> extraMetaData, int blockSize, CompressionCodecName codec) {
    if (writeSupport == null) {
      throw new NullPointerException("writeSupport");
    }
    this.w = w;
    this.writeSupport = writeSupport;
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.blockSize = blockSize;
    this.codec = codec;
    initStore();
  }

  private void initStore() {
    pageStore = new MemPageStore();
    store = new MemColumnsStore(1024 * 1024 * 1, pageStore, 8*1024);
    //
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    writeSupport.initForWrite(columnIO.getRecordWriter(store), schema, extraMetaData);
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
    if (store.memSize() > blockSize) {
      flushStore();
      initStore();
    }
  }

  private void flushStore()
      throws IOException {
    w.startBlock(recordCount);
    store.flush();
    List<ColumnDescriptor> columns = schema.getColumns();
    for (ColumnDescriptor columnDescriptor : columns) {
      PageReader pageReader = pageStore.getPageReader(columnDescriptor);
      int totalValueCount = pageReader.getTotalValueCount();
      w.startColumn(columnDescriptor, totalValueCount, codec);
      int n = 0;
      do {
        Page page = pageReader.readPage();
        n += page.getValueCount();
        // TODO: change INTFC
        w.writeDataPage(page.getValueCount(), (int)page.getBytes().size(), page.getBytes().toByteArray(), 0, (int)page.getBytes().size());
      } while (n < totalValueCount);
      w.endColumn();
    }
    recordCount = 0;
    w.endBlock();
    store = null;
    pageStore = null;
  }
}