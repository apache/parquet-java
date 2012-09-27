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
package redelm.pig;

import java.io.IOException;
import java.util.List;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;

public class RedelmLoader extends LoadFunc {

  private String location;
  private RecordReader reader;

  @Override
  public void setLocation(String location, Job job) throws IOException {
    this.location = location;
  }

  @Override
  public InputFormat<Object, Tuple> getInputFormat() throws IOException {
    return new InputFormat<Object, Tuple>() {

      @Override
      public RecordReader<Object, Tuple> createRecordReader(
          InputSplit inputSplit,
          TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReader<Object, Tuple>() {
          private Tuple currentTuple;
          private int total;
          private int current;
          private boolean dataAvailable;

          private void checkRead() {
          }

          @Override
          public void close() throws IOException {
          }

          @Override
          public Object getCurrentKey() throws IOException,
              InterruptedException {
            return null;
          }

          @Override
          public Tuple getCurrentValue() throws IOException,
              InterruptedException {
            checkRead();
            if (!dataAvailable) {
              throw new IOException("reached end of data");
            }
            return currentTuple;
          }

          @Override
          public float getProgress() throws IOException, InterruptedException {
            return (float)current/total;
          }

          @Override
          public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
              throws IOException, InterruptedException {
          }

          @Override
          public boolean nextKeyValue() throws IOException,
              InterruptedException {
            checkRead();
            return dataAvailable;
          }
        };
      }

      @Override
      public List<InputSplit> getSplits(JobContext jobContext) throws IOException,
          InterruptedException {
//        jobContext.
        return null;
      }
    };
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit split)
      throws IOException {
    this.reader = reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }



}
