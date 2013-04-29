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
package parquet.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;

import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.Footer;

@SuppressWarnings("deprecation")
public class DeprecatedParquetInputFormat<V> extends org.apache.hadoop.mapred.FileInputFormat<Void, Container<V>> {

  protected ParquetInputFormat<V> realInputFormat = new ParquetInputFormat<V>();

  @Override
  public RecordReader<Void, Container<V>> getRecordReader(InputSplit split, JobConf job,
                  Reporter reporter) throws IOException {
    return new RecordReaderWrapper<V>(realInputFormat, split, job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      List<Footer> footers = realInputFormat.getFooters(job, Arrays.asList(super.listStatus(job)));
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

  private static class RecordReaderWrapper<V> implements RecordReader<Void, Container<V>> {

    private ParquetRecordReader<V> realReader;
    private long splitLen; // for getPos()

    private Container<V> valueContainer = null;

    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(ParquetInputFormat<V> newInputFormat,
                               InputSplit oldSplit,
                               JobConf oldJobConf,
                               Reporter reporter) throws IOException {

      splitLen = oldSplit.getLength();

      try {
        realReader = new ParquetRecordReader<V>(newInputFormat.getReadSupport(oldJobConf));
        realReader.initialize(((ParquetInputSplitWrapper)oldSplit).realSplit, oldJobConf);

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



  private static class ParquetInputSplitWrapper implements InputSplit {

    ParquetInputSplit realSplit;


    @SuppressWarnings("unused") // MapReduce instantiates this.
    public ParquetInputSplitWrapper() {}

    public ParquetInputSplitWrapper(ParquetInputSplit realSplit) {
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() throws IOException {
      try {
        return realSplit.getLength();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
    }

    @Override
    public String[] getLocations() throws IOException {
      try {
        return realSplit.getLocations();
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new IOException(e);
      }
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
