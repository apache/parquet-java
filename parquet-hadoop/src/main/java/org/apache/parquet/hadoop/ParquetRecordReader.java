/**
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

import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.SPLIT_FILES;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.vector.ColumnVector;
import org.apache.parquet.vector.RowBatch;
import org.apache.parquet.vector.VectorizedReader;

/**
 * Reads the records from a block of a Parquet file
 *
 * @see ParquetInputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> type of the materialized records
 */
public class ParquetRecordReader<T> extends RecordReader<Void, T> implements VectorizedReader<T> {

  private static final Log LOG = Log.getLog(ParquetRecordReader.class);
  private final InternalParquetRecordReader<T> internalReader;
  private ReadSupport readSupport;
  private ReadSupport.ReadContext readContext;

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   */
  public ParquetRecordReader(ReadSupport<T> readSupport) {
    this(readSupport, FilterCompat.NOOP);
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filter for filtering individual records
   */
  public ParquetRecordReader(ReadSupport<T> readSupport, Filter filter) {
    internalReader = new InternalParquetRecordReader<T>(readSupport, filter);
    this.readSupport = readSupport;
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filter for filtering individual records
   * @deprecated use {@link #ParquetRecordReader(ReadSupport, Filter)}
   */
  @Deprecated
  public ParquetRecordReader(ReadSupport<T> readSupport, UnboundRecordFilter filter) {
    this(readSupport, FilterCompat.get(filter));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    internalReader.close();
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
    return internalReader.getCurrentValue();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return internalReader.getProgress();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (context instanceof TaskInputOutputContext<?, ?, ?, ?>) {
      BenchmarkCounter.initCounterFromContext((TaskInputOutputContext<?, ?, ?, ?>) context);
    } else {
      LOG.error("Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is "
              + context.getClass().getCanonicalName());
    }

    initializeInternalReader(toParquetSplit(inputSplit), ContextUtil.getConfiguration(context));
  }

  public void initialize(InputSplit inputSplit, Configuration configuration, Reporter reporter)
      throws IOException, InterruptedException {
    BenchmarkCounter.initCounterFromReporter(reporter,configuration);
    initializeInternalReader(toParquetSplit(inputSplit), configuration);
  }

  private void initializeInternalReader(ParquetInputSplit split, Configuration configuration) throws IOException {
    Path path = split.getPath();
    long[] rowGroupOffsets = split.getRowGroupOffsets();
    List<BlockMetaData> filteredBlocks;
    ParquetMetadata footer;
    // if task.side.metadata is set, rowGroupOffsets is null
    if (rowGroupOffsets == null) {
      // then we need to apply the predicate push down filter
      footer = readFooter(configuration, path, range(split.getStart(), split.getEnd()));
      MessageType fileSchema = footer.getFileMetaData().getSchema();
      Filter filter = getFilter(configuration);
      filteredBlocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
      footer = readFooter(configuration, path, NO_FILTER);
      Set<Long> offsets = new HashSet<Long>();
      for (long offset : rowGroupOffsets) {
        offsets.add(offset);
      }
      filteredBlocks = new ArrayList<BlockMetaData>();
      for (BlockMetaData block : footer.getBlocks()) {
        if (offsets.contains(block.getStartingPos())) {
          filteredBlocks.add(block);
        }
      }
      // verify we found them all
      if (filteredBlocks.size() != rowGroupOffsets.length) {
        long[] foundRowGroupOffsets = new long[footer.getBlocks().size()];
        for (int i = 0; i < foundRowGroupOffsets.length; i++) {
          foundRowGroupOffsets[i] = footer.getBlocks().get(i).getStartingPos();
        }
        // this should never happen.
        // provide a good error message in case there's a bug
        throw new IllegalStateException(
            "All the offsets listed in the split should be found in the file."
            + " expected: " + Arrays.toString(rowGroupOffsets)
            + " found: " + filteredBlocks
            + " out of: " + Arrays.toString(foundRowGroupOffsets)
            + " in range " + split.getStart() + ", " + split.getEnd());
      }
    }

    if (!filteredBlocks.isEmpty()) {
      checkDeltaByteArrayProblem(footer.getFileMetaData(), configuration, filteredBlocks.get(0));
    }

    MessageType fileSchema = footer.getFileMetaData().getSchema();
    Map<String, String> fileMetaData = footer.getFileMetaData().getKeyValueMetaData();
    readContext = readSupport.init(new InitContext(configuration, InternalParquetRecordReader
        .toSetMultiMap(fileMetaData), fileSchema));

    internalReader.initialize(
        fileSchema, fileMetaData, path, filteredBlocks, configuration);
  }

  private void checkDeltaByteArrayProblem(FileMetaData meta, Configuration conf, BlockMetaData block) {
    // splitting files?
    if (conf.getBoolean(ParquetInputFormat.SPLIT_FILES, true)) {
      // this is okay if not using DELTA_BYTE_ARRAY with the bug
      Set<Encoding> encodings = new HashSet<Encoding>();
      for (ColumnChunkMetaData column : block.getColumns()) {
        encodings.addAll(column.getEncodings());
      }
      for (Encoding encoding : encodings) {
        if (CorruptDeltaByteArrays.requiresSequentialReads(meta.getCreatedBy(), encoding)) {
          throw new ParquetDecodingException("Cannot read data due to " +
              "PARQUET-246: to read safely, set " + SPLIT_FILES + " to false");
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return internalReader.nextKeyValue();
  }

  /**
   * Currently reading a batch of rows of complex types is not supported
   *
   * Reads the next batch of rows. This method is used for reading complex types
   * or arbitrary objects and calls the converters eventually to materialize the record.
   * @param previous a row batch object to be reused by the reader if possible
   * @param clazz the class of the record type that will be filled into the column vector
   * @return the row batch that was read
   * @throws java.io.IOException
   */
  @Override
  public RowBatch nextBatch(RowBatch previous, Class<T> clazz) throws IOException {
    throw new UnsupportedOperationException("Reading a batch of rows of complex types is not supported");
  }

  /**
   * Reads the next batch of rows. This method is used for reading primitive types
   * and does not call the converters at all.
   * @param previous a row batch object to be reused by the reader if possible
   * @return the row batch that was read
   * @throws java.io.IOException
   */
  @Override
  public RowBatch nextBatch(RowBatch previous) throws IOException {
    MessageType requestedSchema = readContext.getRequestedSchema();
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    int nColumns = columns.size();
    ColumnVector[] columnVectors;

    RowBatch rowBatch = previous;
    if (rowBatch == null) {
      rowBatch = new RowBatch();
    }

    if (rowBatch.getColumns() == null) {
      columnVectors = new ColumnVector[nColumns];
      rowBatch.setColumns(columnVectors);
    } else {
      columnVectors = rowBatch.getColumns();
    }

    MessageType[] columnSchemas = new MessageType[nColumns];
    for (int i = 0; i < nColumns; i++) {
      ColumnVector columnVector = columnVectors[i];
      columnSchemas[i] = new MessageType(requestedSchema.getFieldName(i), requestedSchema.getType(i));

      if (columnVector == null) {
        columnVector = ColumnVector.from(columns.get(i));
      }

      rowBatch.getColumns()[i] = columnVector;
    }

    boolean hasMoreRecords = readVectors(rowBatch.getColumns(), columnSchemas);
    return hasMoreRecords ? rowBatch : null;
  }

  private boolean readVectors(ColumnVector[] vectors, MessageType[] columns) throws IOException {
    return internalReader.nextBatch(vectors, columns);
  }

  private ParquetInputSplit toParquetSplit(InputSplit split) throws IOException {
    if (split instanceof ParquetInputSplit) {
      return (ParquetInputSplit) split;
    } else if (split instanceof FileSplit) {
      return ParquetInputSplit.from((FileSplit) split);
    } else if (split instanceof org.apache.hadoop.mapred.FileSplit) {
      return ParquetInputSplit.from(
          (org.apache.hadoop.mapred.FileSplit) split);
    } else {
      throw new IllegalArgumentException(
          "Invalid split (not a FileSplit or ParquetInputSplit): " + split);
    }
  }
}
