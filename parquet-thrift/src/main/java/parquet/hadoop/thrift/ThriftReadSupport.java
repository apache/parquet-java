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
package parquet.hadoop.thrift;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;

import parquet.Log;
import parquet.Strings;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.ParquetDecodingException;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.thrift.TBaseRecordConverter;
import parquet.thrift.ThriftMetaData;
import parquet.thrift.ThriftRecordConverter;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.projection.FieldProjectionFilter;
import parquet.thrift.projection.StrictFieldProjectionFilter;
import parquet.thrift.projection.ThriftProjectionException;
import parquet.thrift.projection.deprecated.DeprecatedFieldProjectionFilter;
import parquet.thrift.struct.ThriftType.StructType;

public class ThriftReadSupport<T> extends ReadSupport<T> {
  private static final Log LOG = Log.getLog(ThriftReadSupport.class);

  /**
   * Deprecated. Use {@link #STRICT_THRIFT_COLUMN_FILTER_KEY}
   * Accepts a ";" delimited list of globs in the syntax implemented by {@link DeprecatedFieldProjectionFilter}
   */
  @Deprecated
  public static final String DEPRECATED_THRIFT_COLUMN_FILTER_KEY = "parquet.thrift.column.filter";

  /**
   * Accepts a ";" delimited list of glob paths, in the syntax implemented by {@link StrictFieldProjectionFilter}
   */
  public static final String STRICT_THRIFT_COLUMN_FILTER_KEY = "parquet.thrift.column.projection.globs";

  private static final String RECORD_CONVERTER_DEFAULT = TBaseRecordConverter.class.getName();
  public static final String THRIFT_READ_CLASS_KEY = "parquet.thrift.read.class";

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified using this key
   * (for example, ScroogeRecordConverter from parquet-scrooge).
   */
  private static final String RECORD_CONVERTER_CLASS_KEY = "parquet.thrift.converter.class";

  protected Class<T> thriftClass;

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified
   * (for example, ScroogeRecordConverter from parquet-scrooge).
   */
  public static void setRecordConverterClass(JobConf conf,
      Class<?> klass) {
    conf.set(RECORD_CONVERTER_CLASS_KEY, klass.getName());
  }

  @Deprecated
  public static void setProjectionPushdown(JobConf jobConf, String projectionString) {
    jobConf.set(DEPRECATED_THRIFT_COLUMN_FILTER_KEY, projectionString);
  }

  public static void setStrictFieldProjectionFilter(Configuration conf, String semicolonDelimitedGlobs) {
    conf.set(STRICT_THRIFT_COLUMN_FILTER_KEY, semicolonDelimitedGlobs);
  }

  public static FieldProjectionFilter getFieldProjectionFilter(Configuration conf) {
    String deprecated = conf.get(DEPRECATED_THRIFT_COLUMN_FILTER_KEY);
    String strict = conf.get(STRICT_THRIFT_COLUMN_FILTER_KEY);

    if (Strings.isNullOrEmpty(deprecated) && Strings.isNullOrEmpty(strict)) {
      return null;
    }

    if(!Strings.isNullOrEmpty(deprecated) && !Strings.isNullOrEmpty(strict)) {
      throw new ThriftProjectionException(
          "You cannot provide both "
              + DEPRECATED_THRIFT_COLUMN_FILTER_KEY
              + " and "
              + STRICT_THRIFT_COLUMN_FILTER_KEY
              +"! "
              + DEPRECATED_THRIFT_COLUMN_FILTER_KEY
              + " is deprecated."
      );
    }

    if (!Strings.isNullOrEmpty(deprecated)) {
      LOG.warn(String.format("Using %s is deprecated. Please see the docs for %s!",
          DEPRECATED_THRIFT_COLUMN_FILTER_KEY, STRICT_THRIFT_COLUMN_FILTER_KEY));
      return new DeprecatedFieldProjectionFilter(deprecated);
    }

    return StrictFieldProjectionFilter.fromSemicolonDelimitedString(strict);
  }

  /**
   * used from hadoop
   * the configuration must contain a "parquet.thrift.read.class" setting
   */
  public ThriftReadSupport() {
  }

  /**
   * @param thriftClass the thrift class used to deserialize the records
   */
  public ThriftReadSupport(Class<T> thriftClass) {
    this.thriftClass = thriftClass;
  }

  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
    final Configuration configuration = context.getConfiguration();
    final MessageType fileMessageType = context.getFileSchema();
    MessageType requestedProjection = fileMessageType;
    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);

    FieldProjectionFilter projectionFilter = getFieldProjectionFilter(configuration);

    if (partialSchemaString != null && projectionFilter != null) {
      throw new ThriftProjectionException(
          String.format("You cannot provide both a partial schema and field projection filter."
                  + "Only one of (%s, %s, %s) should be set.",
              PARQUET_READ_SCHEMA, STRICT_THRIFT_COLUMN_FILTER_KEY, DEPRECATED_THRIFT_COLUMN_FILTER_KEY));
    }

    //set requestedProjections only when it's specified
    if (partialSchemaString != null) {
      requestedProjection = getSchemaForRead(fileMessageType, partialSchemaString);
    } else if (projectionFilter != null) {
      try {
        initThriftClassFromMultipleFiles(context.getKeyValueMetadata(), configuration);
        requestedProjection =  getProjectedSchema(projectionFilter);
      } catch (ClassNotFoundException e) {
        throw new ThriftProjectionException("can not find thriftClass from configuration", e);
      }
    }

    MessageType schemaForRead = getSchemaForRead(fileMessageType, requestedProjection);
    return new ReadContext(schemaForRead);
  }

  @SuppressWarnings("unchecked")
  protected MessageType getProjectedSchema(FieldProjectionFilter fieldProjectionFilter) {
    return new ThriftSchemaConverter(fieldProjectionFilter).convert((Class<TBase<?, ?>>)thriftClass);
  }

  @SuppressWarnings("unchecked")
  private void initThriftClassFromMultipleFiles(Map<String, Set<String>> fileMetadata, Configuration conf) throws ClassNotFoundException {
    if (thriftClass != null) {
      return;
    }
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      Set<String> names = ThriftMetaData.getThriftClassNames(fileMetadata);
      if (names == null || names.size() != 1) {
        throw new ParquetDecodingException("Could not read file as the Thrift class is not provided and could not be resolved from the file: " + names);
      }
      className = names.iterator().next();
    }
    thriftClass = (Class<T>)Class.forName(className);
  }

  @SuppressWarnings("unchecked")
  private void initThriftClass(Map<String, String> fileMetadata, Configuration conf) throws ClassNotFoundException {
    if (thriftClass != null) {
      return;
    }
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      final ThriftMetaData metaData = ThriftMetaData.fromExtraMetaData(fileMetadata);
      if (metaData == null) {
        throw new ParquetDecodingException("Could not read file as the Thrift class is not provided and could not be resolved from the file");
      }
      thriftClass = (Class<T>)metaData.getThriftClass();
    } else {
      thriftClass = (Class<T>)Class.forName(className);
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema,
      parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
    try {
      initThriftClass(keyValueMetaData, configuration);

      String converterClassName = configuration.get(RECORD_CONVERTER_CLASS_KEY, RECORD_CONVERTER_DEFAULT);
      @SuppressWarnings("unchecked")
      Class<ThriftRecordConverter<T>> converterClass = (Class<ThriftRecordConverter<T>>) Class.forName(converterClassName);
      Constructor<ThriftRecordConverter<T>> constructor =
          converterClass.getConstructor(Class.class, MessageType.class, StructType.class);
      ThriftRecordConverter<T> converter = constructor.newInstance(thriftClass, readContext.getRequestedSchema(), thriftMetaData.getDescriptor());
      return converter;
    } catch (Exception t) {
      throw new RuntimeException("Unable to create Thrift Converter for Thrift metadata " + thriftMetaData, t);
    }
  }
}
