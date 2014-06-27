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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import parquet.Log;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.FilterPredicate;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.util.ContextUtil;
import parquet.hadoop.util.counters.BenchmarkCounter;
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

  private static final Log LOG= Log.getLog(ParquetRecordReader.class);
  private InternalParquetRecordReader<T> internalReader;

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   */
  public ParquetRecordReader(ReadSupport<T> readSupport) {
    internalReader = new InternalParquetRecordReader<T>(readSupport, null, null);
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filter filter for only returning matching records.
   */
  public ParquetRecordReader(ReadSupport<T> readSupport, UnboundRecordFilter filter) {
    internalReader = new InternalParquetRecordReader<T>(readSupport, filter, null);
  }

  /**
   * @param readSupport Object which helps reads files of the given type, e.g. Thrift, Avro.
   * @param filterPredicate filter for only returning matching records.
   */
  public ParquetRecordReader(ReadSupport<T> readSupport, FilterPredicate filterPredicate) {
    internalReader = new InternalParquetRecordReader<T>(readSupport, null, filterPredicate);
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
    }else{
      LOG.error("Can not initialize counter due to context is not a instance of TaskInputOutputContext, but is "
              +context.getClass().getCanonicalName());
    }

    initializeInternalReader((ParquetInputSplit)inputSplit, ContextUtil.getConfiguration(context));
  }

  public void initialize(InputSplit inputSplit, Configuration configuration, Reporter reporter)
      throws IOException, InterruptedException {
    BenchmarkCounter.initCounterFromReporter(reporter,configuration);
    initializeInternalReader((ParquetInputSplit) inputSplit, configuration);
  }

  private void initializeInternalReader(ParquetInputSplit split, Configuration configuration) throws IOException {

    internalReader.initialize(
        MessageTypeParser.parseMessageType(split.getRequestedSchema()),
        MessageTypeParser.parseMessageType(split.getFileSchema()),
        split.getExtraMetadata(), split.getReadSupportMetadata(), split.getPath(),
        split.getBlocks(), configuration);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return internalReader.nextKeyValue();
  }
}