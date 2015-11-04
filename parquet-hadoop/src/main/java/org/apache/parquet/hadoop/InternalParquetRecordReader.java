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
package org.apache.parquet.hadoop;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.io.api.RecordMaterializer.RecordMaterializationException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import static java.lang.String.format;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.Preconditions.checkNotNull;
import static org.apache.parquet.hadoop.ParquetInputFormat.STRICT_TYPE_CHECKING;

class InternalParquetRecordReader<T> {
  private static final Log LOG = Log.getLog(InternalParquetRecordReader.class);

  private ColumnIOFactory columnIOFactory = null;
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
  private org.apache.parquet.io.RecordReader<T> recordReader;
  private boolean strictTypeChecking;

  private long totalTimeSpentReadingBytes;
  private long totalTimeSpentProcessingRecords;
  private long startedAssemblingCurrentBlockAt;

  private long totalCountLoadedSoFar = 0;

  private Path file;
  private UnmaterializableRecordCounter unmaterializableRecordCounter;

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
        totalTimeSpentProcessingRecords += (System.currentTimeMillis() - startedAssemblingCurrentBlockAt);
        if (Log.INFO) {
            LOG.info("Assembled and processed " + totalCountLoadedSoFar + " records from " + columnCount + " columns in " + totalTimeSpentProcessingRecords + " ms: "+((float)totalCountLoadedSoFar / totalTimeSpentProcessingRecords) + " rec/ms, " + ((float)totalCountLoadedSoFar * columnCount / totalTimeSpentProcessingRecords) + " cell/ms");
            final long totalTime = totalTimeSpentProcessingRecords + totalTimeSpentReadingBytes;
            if (totalTime != 0) {
                final long percentReading = 100 * totalTimeSpentReadingBytes / totalTime;
                final long percentProcessing = 100 * totalTimeSpentProcessingRecords / totalTime;
                LOG.info("time spent so far " + percentReading + "% reading ("+totalTimeSpentReadingBytes+" ms) and " + percentProcessing + "% processing ("+totalTimeSpentProcessingRecords+" ms)");
            }
        }
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
      if (Log.INFO) LOG.info("block read in memory in " + timeSpentReading + " ms. row count = " + pages.getRowCount());
      if (Log.DEBUG) LOG.debug("initializing Record assembly with requested schema " + requestedSchema);
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, fileSchema, strictTypeChecking);
      recordReader = columnIO.getRecordReader(pages, recordConverter, filter);
      startedAssemblingCurrentBlockAt = System.currentTimeMillis();
      totalCountLoadedSoFar += pages.getRowCount();
      ++ currentBlock;
    }
  }

  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
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

  public void initialize(MessageType fileSchema,
      FileMetaData parquetFileMetadata,
      Path file, List<BlockMetaData> blocks, Configuration configuration)
      throws IOException {
    // initialize a ReadContext for this file
    Map<String, String> fileMetadata = parquetFileMetadata.getKeyValueMetaData();
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        configuration, toSetMultiMap(fileMetadata), fileSchema));
    this.columnIOFactory = new ColumnIOFactory(parquetFileMetadata.getCreatedBy());
    this.requestedSchema = readContext.getRequestedSchema();
    this.fileSchema = fileSchema;
    this.file = file;
    this.columnCount = requestedSchema.getPaths().size();
    this.recordConverter = readSupport.prepareForRead(
        configuration, fileMetadata, fileSchema, readContext);
    this.strictTypeChecking = configuration.getBoolean(STRICT_TYPE_CHECKING, true);
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    reader = new ParquetFileReader(configuration, parquetFileMetadata, file, blocks, columns);
    for (BlockMetaData block : blocks) {
      total += block.getRowCount();
    }
    this.unmaterializableRecordCounter = new UnmaterializableRecordCounter(configuration, total);
    LOG.info("RecordReader initialized will read a total of " + total + " records.");
  }

  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean recordFound = false;

    while (!recordFound) {
      // no more records left
      if (current >= total) { return false; }

      try {
        checkRead();
        current ++;

        try {
          currentValue = recordReader.read();
        } catch (RecordMaterializationException e) {
          // this might throw, but it's fatal if it does.
          unmaterializableRecordCounter.incErrors(e);
          if (DEBUG) LOG.debug("skipping a corrupt record");
          continue;
        }

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

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<K, Set<V>>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<V>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

}
