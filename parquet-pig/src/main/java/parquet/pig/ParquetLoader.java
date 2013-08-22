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

import static parquet.Log.DEBUG;
import static parquet.pig.TupleReadSupport.PARQUET_PIG_REQUESTED_SCHEMA;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.util.ContextUtil;
import parquet.io.ParquetDecodingException;

/**
 *
 * A Pig Loader for the Parquet file format.
 *
 *
 * @author Julien Le Dem
 *
 */
public class ParquetLoader extends LoadFunc implements LoadMetadata, LoadPushDown {
  private static final Log LOG = Log.getLog(ParquetLoader.class);

  private static final Map<String, ParquetInputFormat<Tuple>> inputFormatCache = new HashMap<String, ParquetInputFormat<Tuple>>();

  private final String requestedSchemaStr;
  private Schema requestedSchema;

  private String location;
  private boolean setLocationHasBeenCalled = false;
  private RecordReader<Void, Tuple> reader;
  private ParquetInputFormat<Tuple> parquetInputFormat;
  private Schema schema;

  /**
   * To read the content in its original schema
   */
  public ParquetLoader() {
    this(null);
  }

  /**
   * To read only a subset of the columns in the file
   * @param requestedSchemaStr a subset of the original pig schema in the file
   */
  public ParquetLoader(String requestedSchemaStr) {
    this.requestedSchemaStr = requestedSchemaStr;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    if (DEBUG) LOG.debug("LoadFunc.setLocation(" + location + ", " + job + ")");
    setInput(location, job);
    if (requestedSchema != null) {
      ContextUtil.getConfiguration(job).set(PARQUET_PIG_REQUESTED_SCHEMA, ObjectSerializer.serialize(requestedSchema));
    } else if (requestedSchemaStr != null){
      // request for the full schema (or requestedschema )
      ContextUtil.getConfiguration(job).set(PARQUET_PIG_REQUESTED_SCHEMA, ObjectSerializer.serialize(schema));
    }
  }

  static Schema parsePigSchema(String pigSchemaString) {
    try {
      return pigSchemaString == null ? null : Utils.getSchemaFromString(pigSchemaString);
    } catch (ParserException e) {
      throw new SchemaConversionException("could not parse Pig schema: " + pigSchemaString, e);
    }
  }

  private void setInput(String location, Job job) throws IOException {
    this.setLocationHasBeenCalled  = true;
    this.location = location;
    FileInputFormat.setInputPaths(job, location);
  }

  @Override
  public InputFormat<Void, Tuple> getInputFormat() throws IOException {
    if (DEBUG) LOG.debug("LoadFunc.getInputFormat()");
    return getParquetInputFormat();
  }

  private void checkSetLocationHasBeenCalled() {
    if (!setLocationHasBeenCalled) {
      throw new IllegalStateException("setLocation() must be called first");
    }
  }

  private static class UnregisteringParquetInputFormat extends ParquetInputFormat<Tuple> {

    private final String location;

    public UnregisteringParquetInputFormat(String loction) {
      super(TupleReadSupport.class);
      this.location = loction;
    }

    @Override
    public RecordReader<Void, Tuple> createRecordReader(
        InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      // for local mode we don't want to keep that around
      inputFormatCache.remove(location);
      return super.createRecordReader(inputSplit, taskAttemptContext);
    }
  };

  private ParquetInputFormat<Tuple> getParquetInputFormat() throws ParserException {
    checkSetLocationHasBeenCalled();
    if (parquetInputFormat == null) {
      // unfortunately Pig will create many Loaders, so we cache the inputformat to avoid reading the metadata more than once
      // TODO: check cases where the same location is reused
      parquetInputFormat = inputFormatCache.get(location);
      if (parquetInputFormat == null) {
        parquetInputFormat = new UnregisteringParquetInputFormat(location);
        inputFormatCache.put(location, parquetInputFormat);
      }
    }
    return parquetInputFormat;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split)
      throws IOException {
    if (DEBUG) LOG.debug("LoadFunc.prepareToRead(" + reader + ", " + split + ")");
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
    if (DEBUG) LOG.debug("LoadMetadata.getPartitionKeys(" + location + ", " + job + ")");
    setInput(location, job);
    return null;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    if (DEBUG) LOG.debug("LoadMetadata.getSchema(" + location + ", " + job + ")");
    setInput(location, job);
    if (schema == null) {
      if (requestedSchemaStr == null) {
        // no requested schema => use the schema from the file
        final FileMetaData globalMetaData = getParquetInputFormat().getGlobalMetaData(job);
        schema = TupleReadSupport.getPigSchemaFromFile(globalMetaData.getSchema(), globalMetaData.getKeyValueMetaData());
        if (isElephantBirdCompatible(job)) {
          convertToElephantBirdCompatibleSchema(schema);
        }
      } else {
        // there was a schema requested => use that
        schema = Utils.getSchemaFromString(requestedSchemaStr);
      }
    }
    return new ResourceSchema(schema);
  }

  private void convertToElephantBirdCompatibleSchema(Schema schema) {
    for(FieldSchema fieldSchema:schema.getFields()){
      if (fieldSchema.type== DataType.BOOLEAN) {
        fieldSchema.type=DataType.INTEGER;
      }
    }
  }

  private boolean isElephantBirdCompatible(Job job) {
    return ContextUtil.getConfiguration(job).getBoolean(TupleReadSupport.PARQUET_PIG_ELEPHANT_BIRD_COMPATIBLE, false);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    if (DEBUG) LOG.debug("LoadMetadata.getStatistics(" + location + ", " + job + ")");
    // We do not need to call setInput 
    // as setLocation is guaranteed to be called before this
    long length = 0;
    for (InputSplit split : getParquetInputFormat().getSplits(job)) {
      try {
        length += split.getLength();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted: ", e);
        return null;
      }
    }
    ResourceStatistics stats = new ResourceStatistics();
    stats.setmBytes(length / 1024 / 1024);
    return stats;
  }

  @Override
  public void setPartitionFilter(Expression expression) throws IOException {
    if (DEBUG) LOG.debug("LoadMetadata.setPartitionFilter(" + expression + ")");
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
      throws FrontendException {
    if (requiredFieldList == null)
      return null;
    requestedSchema = getSchemaFromRequiredFieldList(schema, requiredFieldList.getFields());
    return new RequiredFieldResponse(true);
  }

  private Schema getSchemaFromRequiredFieldList(Schema schema, List<RequiredField> fieldList)
      throws FrontendException {
    Schema s = new Schema();
    for (RequiredField rf : fieldList) {
      FieldSchema f;
      try {
         f = schema.getField(rf.getAlias()).clone();
      } catch (CloneNotSupportedException e) {
        throw new FrontendException("Clone not supported for the fieldschema", e);
      }
      if (rf.getSubFields() == null) {
        s.add(f);
      } else {
        Schema innerSchema = getSchemaFromRequiredFieldList(f.schema, rf.getSubFields());
        if (innerSchema == null) {
          return null;
        } else {
          f.schema = innerSchema;
          s.add(f);
        }
      }
    }
    return s;
  }

}
