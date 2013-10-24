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

import static java.util.Arrays.asList;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;
import static parquet.Log.DEBUG;
import static parquet.hadoop.util.ContextUtil.getConfiguration;
import static parquet.pig.PigSchemaConverter.parsePigSchema;
import static parquet.pig.PigSchemaConverter.pigSchemaToString;
import static parquet.pig.TupleReadSupport.PARQUET_PIG_SCHEMA;
import static parquet.pig.TupleReadSupport.getPigSchemaFromMultipleFiles;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
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
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.parser.ParserException;

import parquet.Log;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.metadata.GlobalMetaData;
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

  // Using a weak hash map will ensure that the cache size will be gc'ed when there is memory pressure
  static final Map<String, ParquetInputFormat<Tuple>> inputFormatCache = new WeakHashMap<String, ParquetInputFormat<Tuple>>();

  private Schema requestedSchema;

  private String location;
  private boolean setLocationHasBeenCalled = false;
  private RecordReader<Void, Tuple> reader;
  private ParquetInputFormat<Tuple> parquetInputFormat;
  private Schema schema;
  protected String signature;

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
    this.requestedSchema = parsePigSchema(requestedSchemaStr);
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    if (DEBUG) LOG.debug("LoadFunc.setLocation(" + location + ", " + job + ")");
    setInput(location, job);
    getConfiguration(job).set(PARQUET_PIG_SCHEMA, pigSchemaToString(schema));
  }

  private void setInput(String location, Job job) throws IOException {
    this.setLocationHasBeenCalled  = true;
    this.location = location;
    setInputPaths(job, location);
    initSchema(job);
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
        // Use new string to maintain unreferenced key
        inputFormatCache.put(new String(location), parquetInputFormat);
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
    return new ResourceSchema(schema);
  }

  private void initSchema(Job job) throws IOException {
    if (schema != null) {
      return;
    }
    if (requestedSchema != null) {
      // this is only true in front-end
      schema = requestedSchema;
      return;
    }
    schema = PigSchemaConverter.parsePigSchema(getPropertyFromUDFContext(PARQUET_PIG_SCHEMA));
    if (schema == null) {
      // no requested schema => use the schema from the file
      final GlobalMetaData globalMetaData = getParquetInputFormat().getGlobalMetaData(job);
      schema = getPigSchemaFromMultipleFiles(globalMetaData.getSchema(), globalMetaData.getKeyValueMetaData());
    }
    if (isElephantBirdCompatible(job)) {
      convertToElephantBirdCompatibleSchema(schema);
    }
  }

  private void convertToElephantBirdCompatibleSchema(Schema schema) {
    if (schema == null) {
      return;
    }
    for(FieldSchema fieldSchema:schema.getFields()){
      if (fieldSchema.type== DataType.BOOLEAN) {
        fieldSchema.type=DataType.INTEGER;
      }
      convertToElephantBirdCompatibleSchema(fieldSchema.schema);
    }
  }

  private boolean isElephantBirdCompatible(Job job) {
    return getConfiguration(job).getBoolean(TupleReadSupport.PARQUET_PIG_ELEPHANT_BIRD_COMPATIBLE, false);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    if (DEBUG) LOG.debug("LoadMetadata.getStatistics(" + location + ", " + job + ")");
    // We do not need to call setInput
    // as setLocation is guaranteed to be called before this
    long length = 0;
    try {
      for (InputSplit split : getParquetInputFormat().getSplits(job)) {
        length += split.getLength();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted: ", e);
      return null;
    }
    ResourceStatistics stats = new ResourceStatistics();
    // TODO use pig-0.12 setBytes api when its available
    stats.setmBytes(length / 1024 / 1024);
    return stats;
  }

  @Override
  public void setPartitionFilter(Expression expression) throws IOException {
    if (DEBUG) LOG.debug("LoadMetadata.setPartitionFilter(" + expression + ")");
  }

  @Override
  public List<OperatorSet> getFeatures() {
    return asList(LoadPushDown.OperatorSet.PROJECTION);
  }
  
  protected String getPropertyFromUDFContext(String key) {
    UDFContext udfContext = UDFContext.getUDFContext();
    return udfContext.getUDFProperties(this.getClass(), new String[]{signature}).getProperty(key);
  }
  
  protected Object getFromUDFContext(String key) {
    UDFContext udfContext = UDFContext.getUDFContext();
    return udfContext.getUDFProperties(this.getClass(), new String[]{signature}).get(key);
  }

  protected void storeInUDFContext(String key, Object value) {
    UDFContext udfContext = UDFContext.getUDFContext();
    java.util.Properties props = udfContext.getUDFProperties(
        this.getClass(), new String[]{signature});
    props.put(key, value);
  }

  @Override
  public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList)
      throws FrontendException {
    if (requiredFieldList == null)
      return null;
    schema = getSchemaFromRequiredFieldList(schema, requiredFieldList.getFields());
    storeInUDFContext(PARQUET_PIG_SCHEMA, pigSchemaToString(schema));
    return new RequiredFieldResponse(true);
  }
  
  @Override
  public void setUDFContextSignature(String signature) {
      this.signature = signature;
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
