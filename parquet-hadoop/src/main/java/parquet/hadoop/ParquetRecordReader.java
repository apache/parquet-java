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

import static java.lang.String.format;
import static parquet.Log.DEBUG;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.filter.RecordFilter;
import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.util.ContextUtil;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetDecodingException;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.Type;

/**
 * Reads the records from a block of a Parquet file
 *
 * @see ParquetInputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> type of the materialized records
 */
public class ParquetRecordReader<T> extends RecordReader<Void, T> {
  private static final Log LOG = Log.getLog(ParquetRecordReader.class);

  private final ColumnIOFactory columnIOFactory = new ColumnIOFactory();

  private MessageType requestedSchema;
  private int columnCount;
  private final ReadSupport<T> readSupport;

  private RecordMaterializer<T> recordConverter;

  private T currentValue;
  private long total;
  private int current = 0;
  private int currentBlock = -1;
  private ParquetFileReader reader;
  private parquet.io.RecordReader<T> recordReader;
  private UnboundRecordFilter recordFilter;

  private long totalTimeSpentReadingBytes;
  private long totalTimeSpentProcessingRecords;
  private long startedAssemblingCurrentBlockAt;

  private long totalCountLoadedSoFar = 0;

  /**
   * @param readSupport Provides functionality for reading.
   */
  public ParquetRecordReader(ReadSupport<T> readSupport ) {
    this(readSupport, RecordFilter.NULL_FILTER );
  }

  /**
   * @param readSupport Provides functionality for reading.
   * @param recordFilter Filter to be applied to read records. Use NULL_FILTER if none required.
   */
  public ParquetRecordReader(ReadSupport<T> readSupport, UnboundRecordFilter recordFilter) {
    this.readSupport = readSupport;
    this.recordFilter = recordFilter;
  }

  private void checkRead() throws IOException {
    if (current == totalCountLoadedSoFar) {
      if (current != 0) {
        long timeAssembling = System.currentTimeMillis() - startedAssemblingCurrentBlockAt;
        totalTimeSpentProcessingRecords += timeAssembling;
        LOG.info("Assembled and processed " + totalCountLoadedSoFar + " records from " + columnCount + " columns in " + totalTimeSpentProcessingRecords + " ms: "+((float)totalCountLoadedSoFar / totalTimeSpentProcessingRecords) + " rec/ms, " + ((float)totalCountLoadedSoFar * columnCount / totalTimeSpentProcessingRecords) + " cell/ms");
        long totalTime = totalTimeSpentProcessingRecords + totalTimeSpentReadingBytes;
        long percentReading = 100 * totalTimeSpentReadingBytes / totalTime;
        long percentProcessing = 100 * totalTimeSpentProcessingRecords / totalTime;
        LOG.info("time spent so far " + percentReading + "% reading ("+totalTimeSpentReadingBytes+" ms) and " + percentProcessing + "% processing ("+totalTimeSpentProcessingRecords+" ms)");
      }

      LOG.info("at row " + current + ". reading next block");
      long t0 = System.currentTimeMillis();
      PageReadStore pages = reader.readNextRowGroup();
      if (pages == null) {
        throw new IOException("expecting more rows but reached last block. Read " + current + " out of " + total);
      }
      long timeSpentReading = System.currentTimeMillis() - t0;
      totalTimeSpentReadingBytes += timeSpentReading;
      LOG.info("block read in memory in " + timeSpentReading + " ms. row count = " + pages.getRowCount());
      if (Log.DEBUG) LOG.debug("initializing Record assembly with requested schema " + requestedSchema);
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema);
      recordReader = columnIO.getRecordReader(pages, recordConverter, recordFilter);
      startedAssemblingCurrentBlockAt = System.currentTimeMillis();
      totalCountLoadedSoFar += pages.getRowCount();
      ++ currentBlock;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    reader.close();
  }

  /**
   * always returns null
   */
  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T getCurrentValue() throws IOException,
  InterruptedException {
    return currentValue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float) current / total;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    initialize(inputSplit, ContextUtil.getConfiguration(context));
  }

  public void initialize(InputSplit inputSplit, Configuration configuration)
      throws IOException {
    ParquetInputSplit parquetInputSplit = (ParquetInputSplit)inputSplit;
    this.requestedSchema = MessageTypeParser.parseMessageType(parquetInputSplit.getRequestedSchema());
    this.columnCount = this.requestedSchema.getPaths().size();
    this.recordConverter = readSupport.prepareForRead(
        configuration,
        parquetInputSplit.getExtraMetadata(),
        MessageTypeParser.parseMessageType(parquetInputSplit.getSchema()),
        new ReadSupport.ReadContext(requestedSchema, parquetInputSplit.getReadSupportMetadata()));

    Path path = parquetInputSplit.getPath();
    List<BlockMetaData> blocks = parquetInputSplit.getBlocks();
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    reader = new ParquetFileReader(configuration, path, blocks, columns);
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    LOG.info("RecordReader initialized will read a total of " + total + " records.");
  }

  private boolean contains(GroupType group, String[] path, int index) {
    if (index == path.length) {
      return false;
    }
    if (group.containsField(path[index])) {
      Type type = group.getType(path[index]);
      if (type.isPrimitive()) {
        return index + 1 == path.length;
      } else {
        return contains(type.asGroupType(), path, index + 1);
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (current < total) {
      try {
        checkRead();
        currentValue = recordReader.read();
        if (DEBUG) LOG.debug("read value: " + currentValue);
        current ++;
      } catch (RuntimeException e) {
        throw new ParquetDecodingException(format("Can not read value at %d in block %d", current, currentBlock), e);
      }
      return true;
    }
    return false;
  }
}