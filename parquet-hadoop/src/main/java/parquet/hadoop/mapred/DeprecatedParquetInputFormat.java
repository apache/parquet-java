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
package parquet.hadoop.mapred;

import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;

public class DeprecatedParquetInputFormat<V> extends org.apache.hadoop.mapred.FileInputFormat<Void, Container<V>> {

  protected ParquetInputFormat<V> realInputFormat = new ParquetInputFormat<V>();

  @Override
  public RecordReader<Void, Container<V>> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
    return new RecordReaderWrapper<V>(split, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    if (isTaskSideMetaData(job)) {
      return super.getSplits(job, numSplits);
    }

    List<Footer> footers = getFooters(job);
    List<ParquetInputSplit> splits = realInputFormat.getSplits(job, footers);
    if (splits == null) {
      return null;
    }
    InputSplit[] resultSplits = new InputSplit[splits.size()];
    int i = 0;
    for (ParquetInputSplit split : splits) {
      resultSplits[i++] = new ParquetInputSplitWrapper(split);
    }
    return resultSplits;
  }

  public List<Footer> getFooters(JobConf job) throws IOException {
    return realInputFormat.getFooters(job, asList(super.listStatus(job)));
  }

  private static class RecordReaderWrapper<V> implements RecordReader<Void, Container<V>> {

    private ParquetRecordReader<V> realReader;
    private long splitLen; // for getPos()

    private Container<V> valueContainer = null;

    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(
        InputSplit oldSplit, JobConf oldJobConf, Reporter reporter)
        throws IOException {
      splitLen = oldSplit.getLength();

      try {
        realReader = new ParquetRecordReader<V>(
            ParquetInputFormat.<V>getReadSupportInstance(oldJobConf),
            ParquetInputFormat.getFilter(oldJobConf));

        if (oldSplit instanceof ParquetInputSplitWrapper) {
          realReader.initialize(((ParquetInputSplitWrapper) oldSplit).realSplit, oldJobConf, reporter);
        } else if (oldSplit instanceof FileSplit) {
          realReader.initialize((FileSplit) oldSplit, oldJobConf, reporter);
        } else {
          throw new IllegalArgumentException(
              "Invalid split (not a FileSplit or ParquetInputSplitWrapper): " + oldSplit);
        }

        // read once to gain access to key and value objects
        if (realReader.nextKeyValue()) {
          firstRecord = true;
          valueContainer = new Container<V>();
          valueContainer.set(realReader.getCurrentValue());

        } else {
          eof = true;
        }
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public Container<V> createValue() {
      return valueContainer;
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return realReader.getProgress();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public boolean next(Void key, Container<V> value) throws IOException {
      if (eof) {
        return false;
      }

      if (firstRecord) { // key & value are already read.
        firstRecord = false;
        return true;
      }

      try {
        if (realReader.nextKeyValue()) {
          if (value != null) value.set(realReader.getCurrentValue());
          return true;
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      eof = true; // strictly not required, just for consistency
      return false;
    }
  }

  public static boolean isTaskSideMetaData(JobConf job) {
    return job.getBoolean(ParquetInputFormat.TASK_SIDE_METADATA, TRUE);
  }

  private static class ParquetInputSplitWrapper implements InputSplit {

    ParquetInputSplit realSplit;

    @SuppressWarnings("unused") // MapReduce instantiates this.
    public ParquetInputSplitWrapper() {}

    public ParquetInputSplitWrapper(ParquetInputSplit realSplit) {
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() throws IOException {
        return realSplit.getLength();
    }

    @Override
    public String[] getLocations() throws IOException {
        return realSplit.getLocations();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      realSplit = new ParquetInputSplit();
      realSplit.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      realSplit.write(out);
    }
  }
}
