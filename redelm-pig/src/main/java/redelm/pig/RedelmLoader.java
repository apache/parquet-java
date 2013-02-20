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
import java.util.Map;

import redelm.Log;
import redelm.hadoop.Footer;
import redelm.hadoop.RedelmInputFormat;
import redelm.hadoop.metadata.BlockMetaData;

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

public class RedelmLoader extends LoadFunc implements LoadMetadata {
  private static final Log LOG = Log.getLog(RedelmLoader.class);

  private boolean setLocationHasBeenCalled = false;

  private RecordReader<Void, Tuple> reader;
  private final String schema;

  private RedelmInputFormat<Tuple> redelmInputFormat;


  public RedelmLoader() {
    this.schema = null;
  }

  public RedelmLoader(String schema) {
    this.schema = schema;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    LOG.info("LoadFunc.setLocation(" + location + ", " + job + ")");
    this.setLocationHasBeenCalled  = true;
    FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public InputFormat<Void, Tuple> getInputFormat() throws IOException {
    LOG.info("LoadFunc.getInputFormat()");
    return getRedelmInputFormat();
  }

  private void checkSetLocationHasBeenCalled() {
    if (!setLocationHasBeenCalled) {
      throw new IllegalStateException("setLocation() must be called first");
    }
  }

  private RedelmInputFormat<Tuple> getRedelmInputFormat() throws ParserException {
    checkSetLocationHasBeenCalled();
    if (redelmInputFormat == null) {
      redelmInputFormat = new RedelmInputFormat<Tuple>(
          TupleReadSupport.class,
          schema == null ? null :
            new PigSchemaConverter().convert(Utils.getSchemaFromString(schema)).toString());
    }
    return redelmInputFormat;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
      throws IOException {
    LOG.info("LoadFunc.prepareToRead(" + reader + ", " + split + ")");
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
    LOG.info("LoadMetadata.getPartitionKeys(" + location + ", " + job + ")");
    setLocation(location, job);
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    LOG.info("LoadMetadata.getSchema(" + location + ", " + job + ")");
    setLocation(location, job);
    final List<Footer> footers = getRedelmInputFormat().getFooters(job);
    String pigSchemaString = null;
    if (schema != null) {
      pigSchemaString = schema;
    } else {
      for (Footer footer : footers) {
        PigMetaData pigMetaData = PigMetaData.fromMetaDataBlocks(footer.getRedelmMetaData().getKeyValueMetaData());
        if (pigSchemaString == null) {
          pigSchemaString = pigMetaData.getPigSchema();
        } else {
          if (!pigSchemaString.equals(pigMetaData.getPigSchema())) {
            throw new RuntimeException("all files must have the same pig schema: " + pigSchemaString + " != " + pigMetaData.getPigSchema());
          }
        }
      }
    }
    return new ResourceSchema(Utils.getSchemaFromString(pigSchemaString));
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    LOG.info("LoadMetadata.getStatistics(" + location + ", " + job + ")");
    setLocation(location, job);
    return null;
  }

  @Override
  public void setPartitionFilter(Expression expression) throws IOException {
    LOG.info("LoadMetadata.setPartitionFilter(" + expression + ")");
  }

}
