/**
 * Copyright 2014 Twitter, Inc.
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
package parquet.proto.pig;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.Tuple;

import parquet.hadoop.ParquetOutputFormat;
import parquet.io.ParquetEncodingException;

/**
 * To store in Pig using a proto class
 * usage:
 * STORE 'foo' USING parquet.proto.pig.ParquetProtoStorer('my.proto.Class');
 *
 * @author Peter Lin
 *
 */
public class ParquetProtoStorer extends StoreFunc {

  private RecordWriter<Void, Tuple> recordWriter;

  private String className;

  public ParquetProtoStorer(String[] params) {
    if (params == null || params.length != 1) {
      throw new IllegalArgumentException("required the proto class name in parameter. Got " + Arrays.toString(params) + " instead");
    }
    className = params[0];
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OutputFormat<Void, Tuple> getOutputFormat() throws IOException {
    return new ParquetOutputFormat<Tuple>(new TupleToProtoWriteSupport(className));
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings({ "rawtypes", "unchecked" }) // that's how the base class is defined
  @Override
  public void prepareToWrite(RecordWriter recordWriter) throws IOException {
    this.recordWriter = recordWriter;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putNext(Tuple tuple) throws IOException {
    try {
      this.recordWriter.write(null, tuple);
    } catch (InterruptedException e) {
      throw new ParquetEncodingException("Interrupted while writing", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

}
