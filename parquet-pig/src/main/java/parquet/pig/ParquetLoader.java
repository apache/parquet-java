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
package parquet.pig;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetInputFormat;

public class ParquetLoader extends LoadFunc implements LoadMetadata {
  private static final Log LOG = Log.getLog(ParquetLoader.class);

  private final String requestedSchema;

  private boolean setLocationHasBeenCalled = false;
  private RecordReader<Void, Tuple> reader;
  private ParquetInputFormat<Tuple> parquetInputFormat;
  private String schema;

  public ParquetLoader() {
    this.requestedSchema = null;
  }

  public ParquetLoader(String requestedSchema) {
    this.requestedSchema = requestedSchema;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    LOG.debug("LoadFunc.setLocation(" + location + ", " + job + ")");
    setInput(location, job);
  }

  private void setInput(String location, Job job) throws IOException {
    this.setLocationHasBeenCalled  = true;
    FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public InputFormat<Void, Tuple> getInputFormat() throws IOException {
    LOG.debug("LoadFunc.getInputFormat()");
    return getParquetInputFormat();
  }

  private void checkSetLocationHasBeenCalled() {
    if (!setLocationHasBeenCalled) {
      throw new IllegalStateException("setLocation() must be called first");
    }
  }

  private ParquetInputFormat<Tuple> getParquetInputFormat() throws ParserException {
    checkSetLocationHasBeenCalled();
    if (parquetInputFormat == null) {
      parquetInputFormat = new ParquetInputFormat<Tuple>(
          TupleReadSupport.class,
          requestedSchema == null ? null :
            new PigSchemaConverter().convert(Utils.getSchemaFromString(requestedSchema)).toString());
    }
    return parquetInputFormat;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
      throws IOException {
    LOG.debug("LoadFunc.prepareToRead(" + reader + ", " + split + ")");
    this.reader = reader;
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if (reader.nextKeyValue()) {
        return (Tuple)reader.getCurrentValue();
      } else {
        return null;
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new RuntimeException("Interrupted", e);
    }
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    LOG.debug("LoadMetadata.getPartitionKeys(" + location + ", " + job + ")");
    setInput(location, job);
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    LOG.debug("LoadMetadata.getSchema(" + location + ", " + job + ")");
    setInput(location, job);
    final List<Footer> footers = getParquetInputFormat().getFooters(job);
    if (schema == null) {
      if (requestedSchema == null) {
        String pigSchemaString = null;
        for (Footer footer : footers) {
          PigMetaData pigMetaData = PigMetaData.fromMetaData(footer.getParquetMetadata().getKeyValueMetaData());
          if (pigSchemaString == null) {
            pigSchemaString = pigMetaData.getPigSchema();
          } else {
            if (!pigSchemaString.equals(pigMetaData.getPigSchema())) {
              throw new RuntimeException("all files must have the same pig schema: " + pigSchemaString + " != " + pigMetaData.getPigSchema());
            }
          }
        }
        schema = pigSchemaString;
      } else {
        schema = requestedSchema;
      }
    }
    return new ResourceSchema(Utils.getSchemaFromString(schema));
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    LOG.debug("LoadMetadata.getStatistics(" + location + ", " + job + ")");
    setInput(location, job);
    return null;
  }

  @Override
  public void setPartitionFilter(Expression expression) throws IOException {
    LOG.debug("LoadMetadata.setPartitionFilter(" + expression + ")");
  }

}
