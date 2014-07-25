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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.page.PageReadStore;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.util.counters.BenchmarkCounter;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.ParquetDecodingException;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

import static java.lang.String.format;
import static parquet.Log.DEBUG;
import static parquet.Preconditions.checkNotNull;
import static parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

class InternalParquetRecordReader<T> {
  private static final Log LOG = Log.getLog(InternalParquetRecordReader.class);

  private final ColumnIOFactory columnIOFactory = new ColumnIOFactory();
  private final Filter filter;

  private MessageType requestedSchema;
  private MessageType fileSchema;
  private int columnCount;
  private final ReadSupport<T> readSupport;

  private RecordMaterializer<T> recordConverter;

  private T currentValue;
  private long total;
  private long current = 0;
  private int currentBlock = -1;
  private ParquetFileReader reader;
  private parquet.io.RecordReader<T> recordReader;
  private boolean strictTypeChecking;

  private long totalTimeSpentReadingBytes;
  private long totalTimeSpentProcessingRecords;
  private long startedAssemblingCurrentBlockAt;

  private long totalCountLoadedSoFar = 0;

  private Path file;

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filter for filtering individual records
   */
  public InternalParquetRecordReader(ReadSupport<T> readSupport, Filter filter) {
    this.readSupport = readSupport;
    this.filter = checkNotNull(filter, "filter");
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   */
  public InternalParquetRecordReader(ReadSupport<T> readSupport) {
    this(readSupport, FilterCompat.NOOP);
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filter Optional filter for only returning matching records.
   * @deprecated use {@link #InternalParquetRecordReader(ReadSupport, Filter)}
   */
  @Deprecated
  public InternalParquetRecordReader(ReadSupport<T> readSupport, UnboundRecordFilter filter) {
    this(readSupport, FilterCompat.get(filter));
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
      BenchmarkCounter.incrementTime(timeSpentReading);
      LOG.info("block read in memory in " + timeSpentReading + " ms. row count = " + pages.getRowCount());
      if (Log.DEBUG) LOG.debug("initializing Record assembly with requested schema " + requestedSchema);
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
      recordReader = columnIO.getRecordReader(pages, recordConverter, filter);
      startedAssemblingCurrentBlockAt = System.currentTimeMillis();
      totalCountLoadedSoFar += pages.getRowCount();
      ++ currentBlock;
    }
  }

  public void close() throws IOException {
    reader.close();
  }

  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  public T getCurrentValue() throws IOException,
  InterruptedException {
    return currentValue;
  }

  public float getProgress() throws IOException, InterruptedException {
    return (float) current / total;
  }

  public void initialize(MessageType requestedSchema, MessageType fileSchema,
      Map<String, String> extraMetadata, Map<String, String> readSupportMetadata,
      Path file, List<BlockMetaData> blocks, Configuration configuration)
      throws IOException {
    this.requestedSchema = requestedSchema;
    this.fileSchema = fileSchema;
    this.file = file;
    this.columnCount = this.requestedSchema.getPaths().size();
    this.recordConverter = readSupport.prepareForRead(
        configuration, extraMetadata, fileSchema,
        new ReadSupport.ReadContext(requestedSchema, readSupportMetadata));
    this.strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    reader = new ParquetFileReader(configuration, file, blocks, columns);
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

  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean recordFound = false;

    while (!recordFound) {
      // no more records left
      if (current >= total) { return false; }

      try {
        checkRead();
        currentValue = recordReader.read();
        current ++;
        if (recordReader.shouldSkipCurrentRecord()) {
          // this record is being filtered via the filter2 package
          if (DEBUG) LOG.debug("skipping record");
          continue;
        }

        if (currentValue == null) {
          // only happens with FilteredRecordReader at end of block
          current = totalCountLoadedSoFar;
          if (DEBUG) LOG.debug("filtered record reader reached end of block");
          continue;
        }

        recordFound = true;

        if (DEBUG) LOG.debug("read value: " + currentValue);
      } catch (RuntimeException e) {
        throw new ParquetDecodingException(format("Can not read value at %d in block %d in file %s", current, currentBlock, file), e);
      }
    }
    return true;
  }
}