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
import java.util.Map;

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
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.metadata.FileMetaData;
import parquet.io.ParquetDecodingException;

/**
 *
 * A Pig Loader for the Parquet file format.
 *
 *
 * @author Julien Le Dem
 *
 */
public class ParquetLoader extends LoadFunc implements LoadMetadata {
  private static final Log LOG = Log.getLog(ParquetLoader.class);

  private final String requestedSchema;

  private boolean setLocationHasBeenCalled = false;
  private RecordReader<Void, Tuple> reader;
  private ParquetInputFormat<Tuple> parquetInputFormat;
  private Schema schema;

  /**
   * To read the content in its original schema
   */
  public ParquetLoader() {
    this.requestedSchema = null;
  }

  /**
   * To read only a subset of the columns in the file
   * @param requestedSchema a subset of the original pig schema in the file
   */
  public ParquetLoader(String requestedSchema) {
    this.requestedSchema = requestedSchema;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    LOG.debug("LoadFunc.setLocation(" + location + ", " + job + ")");
    setInput(location, job);
    if (requestedSchema != null) {
      job.getConfiguration().set("parquet.pig.requested.schema", requestedSchema);
    }
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
      parquetInputFormat = new ParquetInputFormat<Tuple>(TupleReadSupport.class);
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
      throw new ParquetDecodingException("Interrupted", e);
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
    if (schema == null) {
      if (requestedSchema == null) {
        // no requested schema => use the schema from the file
        final FileMetaData globalMetaData = getParquetInputFormat().getGlobalMetaData(job);
        // TODO: if no Pig schema in file: generate one from the Parquet schema
        schema = TupleReadSupport.getPigSchemaFromFile(globalMetaData.getSchema(), globalMetaData.getKeyValueMetaData());
      } else {
        // there was a schema requested => use that
        schema = Utils.getSchemaFromString(requestedSchema);
      }
    }
    return new ResourceSchema(schema);
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
