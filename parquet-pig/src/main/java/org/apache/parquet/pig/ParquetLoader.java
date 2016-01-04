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
package org.apache.parquet.pig;

import static java.util.Arrays.asList;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths;
import static org.apache.parquet.Log.DEBUG;
import static org.apache.parquet.hadoop.util.ContextUtil.getConfiguration;
import static org.apache.parquet.pig.PigSchemaConverter.parsePigSchema;
import static org.apache.parquet.pig.PigSchemaConverter.pigSchemaToString;
import static org.apache.parquet.pig.PigSchemaConverter.serializeRequiredFieldList;
import static org.apache.parquet.pig.TupleReadSupport.PARQUET_PIG_SCHEMA;
import static org.apache.parquet.pig.TupleReadSupport.PARQUET_PIG_REQUIRED_FIELDS;
import static org.apache.parquet.pig.TupleReadSupport.PARQUET_COLUMN_INDEX_ACCESS;
import static org.apache.parquet.pig.TupleReadSupport.getPigSchemaFromMultipleFiles;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.util.SerializationUtil;
import org.apache.parquet.io.api.Binary;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPredicatePushdown;
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

import static org.apache.pig.Expression.*;

import org.apache.parquet.Log;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.GlobalMetaData;
import org.apache.parquet.io.ParquetDecodingException;

/**
 *
 * A Pig Loader for the Parquet file format.
 *
 *
 * @author Julien Le Dem
 *
 */
public class ParquetLoader extends LoadFunc implements LoadMetadata, LoadPushDown, LoadPredicatePushdown {
  private static final Log LOG = Log.getLog(ParquetLoader.class);

  public static final String ENABLE_PREDICATE_FILTER_PUSHDOWN = "parquet.pig.predicate.pushdown.enable";
  private static final boolean DEFAULT_PREDICATE_PUSHDOWN_ENABLED = false;

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

    FilterPredicate filterPredicate = (FilterPredicate) getFromUDFContext(ParquetInputFormat.FILTER_PREDICATE);
    if(filterPredicate != null) {
      ParquetInputFormat.setFilterPredicate(getConfiguration(job), filterPredicate);
    }
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

  @Override
  public List<String> getPredicateFields(String s, Job job) throws IOException {
    if(!job.getConfiguration().getBoolean(ENABLE_PREDICATE_FILTER_PUSHDOWN, DEFAULT_PREDICATE_PUSHDOWN_ENABLED)) {
      return null;
    }

    List<String> fields = new ArrayList<String>();

    for(FieldSchema field : schema.getFields()) {
      switch(field.type) {
        case DataType.BOOLEAN:
        case DataType.INTEGER:
        case DataType.LONG:
        case DataType.FLOAT:
        case DataType.DOUBLE:
        case DataType.CHARARRAY:
          fields.add(field.alias);
          break;
        default:
          // Skip BYTEARRAY, TUPLE, MAP, BAG, DATETIME, BIGINTEGER, BIGDECIMAL
          break;
      }
    }

    return fields;
  }

  @Override
  public List<Expression.OpType> getSupportedExpressionTypes() {
    Expression.OpType supportedTypes [] = {
        Expression.OpType.OP_EQ,
        Expression.OpType.OP_GT,
        Expression.OpType.OP_GE,
        Expression.OpType.OP_LT,
        Expression.OpType.OP_LE,
        Expression.OpType.OP_AND,
        Expression.OpType.OP_OR,
    };

    return Arrays.asList(supportedTypes);
  }

  @Override
  public void setPushdownPredicate(Expression e) throws IOException {
    LOG.info("Pig pushdown expression: " + e);

    FilterPredicate pred = buildFilter(e);
    LOG.info("Parquet filter predicate expression: " + pred);

    storeInUDFContext(ParquetInputFormat.FILTER_PREDICATE, pred);
  }

  private FilterPredicate buildFilter(Expression e) {
    if (e instanceof BinaryExpression) {
      Expression lhs = ((BinaryExpression) e).getLhs();
      Expression rhs = ((BinaryExpression) e).getRhs();
      OpType op = e.getOpType();

      FilterPredicate lfp;
      FilterPredicate rfp;
      switch (op) {
        case OP_AND:
          lfp = buildFilter(lhs);
          rfp = buildFilter(rhs);
          if (lfp == null || rfp == null) {
            return null;
          }
          return and(lfp, rfp);
        case OP_OR:
          lfp = buildFilter(lhs);
          rfp = buildFilter(rhs);
          if (lfp == null || rfp == null) {
            return null;
          }
          return or(lfp, rfp);
      }

      if (lhs instanceof Column && rhs instanceof Const) {
        return buildFilter(op, (Column) lhs, (Const) rhs);
      } else if (lhs instanceof Const && rhs instanceof Column) {
        return buildFilter(op, (Column) rhs, (Const) lhs);
      }
    }

    return null;
  }

  private FilterPredicate buildFilter(OpType op, Column col, Const value) {
    String name = col.getName();
    try {
      FieldSchema f = schema.getField(name);
      switch (f.type) {
//        case DataType.BOOLEAN:
//          Operators.BooleanColumn col = booleanColumn(name);
//          return op(op, col, value);
        case DataType.INTEGER:
          Operators.IntColumn intCol = intColumn(name);
          return op(op, intCol, value);
        case DataType.LONG:
          Operators.LongColumn longCol = longColumn(name);
          return op(op, longCol, value);
        case DataType.FLOAT:
          Operators.FloatColumn floatCol = floatColumn(name);
          return op(op, floatCol, value);
        case DataType.DOUBLE:
          Operators.DoubleColumn doubleCol = doubleColumn(name);
          return op(op, doubleCol, value);
        case DataType.CHARARRAY:
          Operators.BinaryColumn binaryCol = binaryColumn(name);
          return op(op, binaryCol, value);

      }
    } catch (FrontendException e) {
      throw new RuntimeException("Error processing pushdown for column:" + col, e);
    }

    return null;
  }

  private <C extends Comparable<C>, COL extends Operators.Column<C> & Operators.SupportsLtGt>
  FilterPredicate op(Expression.OpType op, COL col, Expression valueExpr) {
    C value = getValue(valueExpr, col.getColumnType());
    switch (op) {
      case OP_EQ: return eq(col, value);
      case OP_GT: return gt(col, value);
      case OP_GE: return gtEq(col, value);
      case OP_LT: return lt(col, value);
      case OP_LE: return ltEq(col, value);
    }
    return null;
  }

  private <C extends Comparable<C>> C getValue(Expression expr, Class<C> type) {
    Comparable value = (Comparable) ((Const) expr).getValue();

    if (value instanceof String) {
      value = Binary.fromString((String) value);
    }

    return type.cast(value);
  }

}
