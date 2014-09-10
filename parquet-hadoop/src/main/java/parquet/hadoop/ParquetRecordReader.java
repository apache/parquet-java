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

import static parquet.format.converter.ParquetMetadataConverter.range;

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

import parquet.Log;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.compat.RowGroupFilter;
import parquet.filter2.compat.FilterCompat.Filter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.hadoop.util.counters.BenchmarkCounter;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

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
  private final InternalParquetRecordReader<T> internalReader;

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

    initializeInternalReader((ParquetInputSplit)inputSplit, ContextUtil.getConfiguration(context));
  }

  public void initialize(InputSplit inputSplit, Configuration configuration, Reporter reporter)
      throws IOException, InterruptedException {
    BenchmarkCounter.initCounterFromReporter(reporter,configuration);
    initializeInternalReader((ParquetInputSplit) inputSplit, configuration);
  }

  private void initializeInternalReader(ParquetInputSplit split, Configuration configuration) throws IOException {
    Path path = split.getPath();
    ParquetMetadata footer = ParquetFileReader.readFooter(
        configuration, path, range(split.getStart(), split.getEnd()));
    long[] rowGroupOffsets = split.getRowGroupOffsets();
    List<BlockMetaData> filteredBlocks;
    // if task.side.metadata is set, rowGroupOffsets is null
    MessageType fileSchema = footer.getFileMetaData().getSchema();
    if (rowGroupOffsets == null) {
      // then we need to apply the predicate push down filter
      Filter filter = ParquetInputFormat.getFilter(configuration);
      filteredBlocks = RowGroupFilter.filterRowGroups(filter, footer.getBlocks(), fileSchema);
    } else {
      // otherwise we find the row groups that were selected on the client
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
    MessageType requestedSchema = MessageTypeParser.parseMessageType(split.getRequestedSchema());
    Map<String, String> fileMetaData = footer.getFileMetaData().getKeyValueMetaData();
    Map<String, String> readSupportMetadata = split.getReadSupportMetadata();
    internalReader.initialize(
        requestedSchema,fileSchema,
        fileMetaData, readSupportMetadata,
        path,
        filteredBlocks, configuration);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return internalReader.nextKeyValue();
  }
}