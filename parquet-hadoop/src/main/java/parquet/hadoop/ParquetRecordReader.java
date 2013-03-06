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
import parquet.column.mem.PageReadStore;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.convert.RecordConverter;
import parquet.parser.MessageTypeParser;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
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

  private final MessageType requestedSchema;
  private final int columnCount;
  private final Class<?> readSupportClass;

  private RecordConverter<T> recordConverter;

  private T currentValue;
  private long total;
  private int current = 0;
  private ParquetFileReader reader;
  private parquet.io.RecordReader<T> recordReader;

  private long totalTimeSpentReadingBytes;
  private long totalTimeSpentProcessingRecords;
  private long startedAssemblingCurrentBlockAt;

  private long totalCountLoadedSoFar = 0;



  /**
   *
   * @param requestedSchema the requested schema (a subset of the original schema) for record projection
   * @param readSupportClass
   */
  ParquetRecordReader(String requestedSchema, Class<?> readSupportClass) {
    this.requestedSchema = MessageTypeParser.parseMessageType(requestedSchema);
    this.columnCount = this.requestedSchema.getPaths().size();
    this.readSupportClass = readSupportClass;
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
      recordReader = columnIO.getRecordReader(pages, recordConverter);
      startedAssemblingCurrentBlockAt = System.currentTimeMillis();
      totalCountLoadedSoFar += pages.getRowCount();
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
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Configuration configuration = taskAttemptContext.getConfiguration();
    @SuppressWarnings("unchecked") // I know
    ParquetInputSplit<T> parquetInputSplit = (ParquetInputSplit<T>)inputSplit;
    try {
      @SuppressWarnings("unchecked") // I know
      ReadSupport<T> readSupport = (ReadSupport<T>)readSupportClass.newInstance();
      this.recordConverter = readSupport.initForRead(
          configuration,
          parquetInputSplit.getExtraMetadata(),
          MessageTypeParser.parseMessageType(parquetInputSplit.getSchema()),
          requestedSchema);
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instanciate read support class", e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instanciate read support class", e);
    }
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
      checkRead();
      currentValue = recordReader.read();
      if (DEBUG) LOG.debug("read value: " + currentValue);
      current ++;
      return true;
    }
    return false;
  }
}