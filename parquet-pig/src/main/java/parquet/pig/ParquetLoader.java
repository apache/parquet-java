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
package parquet.pig;

import static java.util.Arrays.asList;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;
import static parquet.Log.DEBUG;
import static parquet.hadoop.util.ContextUtil.getConfiguration;
import static parquet.pig.PigSchemaConverter.parsePigSchema;
import static parquet.pig.PigSchemaConverter.pigSchemaToString;
import static parquet.pig.PigSchemaConverter.serializeRequiredFieldList;
import static parquet.pig.TupleReadSupport.PARQUET_PIG_SCHEMA;
import static parquet.pig.TupleReadSupport.PARQUET_PIG_REQUIRED_FIELDS;
import static parquet.pig.TupleReadSupport.PARQUET_COLUMN_INDEX_ACCESS;
import static parquet.pig.TupleReadSupport.getPigSchemaFromMultipleFiles;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
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

  // Using a weak hash map will ensure that the cache will be gc'ed when there is memory pressure
  static final Map<String, Reference<ParquetInputFormat<Tuple>>> inputFormatCache = new WeakHashMap<String, Reference<ParquetInputFormat<Tuple>>>();

  private Schema requestedSchema;
  private boolean columnIndexAccess;

  private String location;
  private boolean setLocationHasBeenCalled = false;
  private RecordReader<Void, Tuple> reader;
  private ParquetInputFormat<Tuple> parquetInputFormat;
  private Schema schema;
  private RequiredFieldList requiredFieldList = null;
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
    this(parsePigSchema(requestedSchemaStr), false);
  }

  /**
   * To read only a subset of the columns in the file optionally assigned by
   * column positions.  Using column positions allows for renaming the fields
   * and is more inline with the "schema-on-read" approach to accessing file
   * data.
   *
   * Example:
   * File Schema:  'c1:int, c2:float, c3:double, c4:long'
   * ParquetLoader('n1:int, n2:float, n3:double, n4:long', 'true');
   *
   * This will use the names provided in the requested schema and assign them
   * to column positions indicated by order.
   *
   * @param requestedSchemaStr a subset of the original pig schema in the file
   * @param columnIndexAccess use column index positions as opposed to name (default: false)
   */
  public ParquetLoader(String requestedSchemaStr, String columnIndexAccess) {
    this(parsePigSchema(requestedSchemaStr), Boolean.parseBoolean(columnIndexAccess));
  }

  /**
   * Use the provided schema to access the underlying file data.
   *
   * The same as the string based constructor but for programmatic use.
   *
   * @param requestedSchema a subset of the original pig schema in the file
   * @param columnIndexAccess
   */
  public ParquetLoader(Schema requestedSchema, boolean columnIndexAccess) {
    this.requestedSchema = requestedSchema;
    this.columnIndexAccess = columnIndexAccess;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    if (DEBUG) LOG.debug("LoadFunc.setLocation(" + location + ", " + job + ")");

    setInput(location, job);
  }

  private void setInput(String location, Job job) throws IOException {
    this.setLocationHasBeenCalled  = true;
    this.location = location;
    setInputPaths(job, location);

    //This is prior to load because the initial value comes from the constructor
    //not file metadata or pig framework and would get overwritten in initSchema().
    if(UDFContext.getUDFContext().isFrontend()) {
      storeInUDFContext(PARQUET_COLUMN_INDEX_ACCESS, Boolean.toString(columnIndexAccess));
    }

    schema = PigSchemaConverter.parsePigSchema(getPropertyFromUDFContext(PARQUET_PIG_SCHEMA));
    requiredFieldList = PigSchemaConverter.deserializeRequiredFieldList(getPropertyFromUDFContext(PARQUET_PIG_REQUIRED_FIELDS));
    columnIndexAccess = Boolean.parseBoolean(getPropertyFromUDFContext(PARQUET_COLUMN_INDEX_ACCESS));

    initSchema(job);

    if(UDFContext.getUDFContext().isFrontend()) {
      //Setting for task-side loading via initSchema()
      storeInUDFContext(PARQUET_PIG_SCHEMA, pigSchemaToString(schema));
      storeInUDFContext(PARQUET_PIG_REQUIRED_FIELDS, serializeRequiredFieldList(requiredFieldList));
    }

    //Used by task-side loader via TupleReadSupport
    getConfiguration(job).set(PARQUET_PIG_SCHEMA, pigSchemaToString(schema));
    getConfiguration(job).set(PARQUET_PIG_REQUIRED_FIELDS, serializeRequiredFieldList(requiredFieldList));
    getConfiguration(job).set(PARQUET_COLUMN_INDEX_ACCESS, Boolean.toString(columnIndexAccess));
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

    public UnregisteringParquetInputFormat(String location) {
      super(TupleReadSupport.class);
      this.location = location;
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
      Reference<ParquetInputFormat<Tuple>> ref = inputFormatCache.get(location);
      parquetInputFormat = ref == null ? null : ref.get();
      if (parquetInputFormat == null) {
        parquetInputFormat = new UnregisteringParquetInputFormat(location);
        inputFormatCache.put(location, new SoftReference<ParquetInputFormat<Tuple>>(parquetInputFormat));
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
    if (schema == null && requestedSchema != null) {
      // this is only true in front-end
      schema = requestedSchema;
    }
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
    /* We need to call setInput since setLocation is not
       guaranteed to be called before this */
    setInput(location, job);
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
    this.requiredFieldList = requiredFieldList;

    if (requiredFieldList == null)
      return null;

    schema = getSchemaFromRequiredFieldList(schema, requiredFieldList.getFields());
    storeInUDFContext(PARQUET_PIG_SCHEMA, pigSchemaToString(schema));
    storeInUDFContext(PARQUET_PIG_REQUIRED_FIELDS, serializeRequiredFieldList(requiredFieldList));

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
