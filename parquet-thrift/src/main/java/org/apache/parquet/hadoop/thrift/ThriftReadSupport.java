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
package org.apache.parquet.hadoop.thrift;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;

import org.apache.parquet.Strings;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.TBaseRecordConverter;
import org.apache.parquet.thrift.ThriftMetaData;
import org.apache.parquet.thrift.ThriftRecordConverter;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.projection.FieldProjectionFilter;
import org.apache.parquet.thrift.projection.StrictFieldProjectionFilter;
import org.apache.parquet.thrift.projection.ThriftProjectionException;
import org.apache.parquet.thrift.projection.deprecated.DeprecatedFieldProjectionFilter;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftReadSupport<T> extends ReadSupport<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftReadSupport.class);

  /**
   * Deprecated. Use {@link #STRICT_THRIFT_COLUMN_FILTER_KEY}
   * Accepts a ";" delimited list of globs in the syntax implemented by {@link DeprecatedFieldProjectionFilter}
   */
  @Deprecated
  public static final String THRIFT_COLUMN_FILTER_KEY = "parquet.thrift.column.filter";

  /**
   * Accepts a ";" delimited list of glob paths, in the syntax implemented by {@link StrictFieldProjectionFilter}
   */
  public static final String STRICT_THRIFT_COLUMN_FILTER_KEY = "parquet.thrift.column.projection.globs";

  private static final String RECORD_CONVERTER_DEFAULT = TBaseRecordConverter.class.getName();
  public static final String THRIFT_READ_CLASS_KEY = "parquet.thrift.read.class";

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified using this key.
   */
  private static final String RECORD_CONVERTER_CLASS_KEY = "parquet.thrift.converter.class";

  protected Class<T> thriftClass;

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified.
   *
   * @param conf a mapred jobconf
   * @param klass a thrift class
   * @deprecated use {@link #setRecordConverterClass(Configuration, Class)} below
   */
  @Deprecated
  public static void setRecordConverterClass(JobConf conf,
      Class<?> klass) {
    setRecordConverterClass((Configuration) conf, klass);
  }

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified.
   *
   * @param conf a configuration
   * @param klass a thrift class
   */
  public static void setRecordConverterClass(Configuration conf,
                                             Class<?> klass) {
    conf.set(RECORD_CONVERTER_CLASS_KEY, klass.getName());
  }

  @Deprecated
  public static void setProjectionPushdown(JobConf jobConf, String projectionString) {
    jobConf.set(THRIFT_COLUMN_FILTER_KEY, projectionString);
  }

  public static void setStrictFieldProjectionFilter(Configuration conf, String semicolonDelimitedGlobs) {
    conf.set(STRICT_THRIFT_COLUMN_FILTER_KEY, semicolonDelimitedGlobs);
  }

  public static FieldProjectionFilter getFieldProjectionFilter(Configuration conf) {
    return getFieldProjectionFilter(new HadoopParquetConfiguration(conf));
  }

  public static FieldProjectionFilter getFieldProjectionFilter(ParquetConfiguration conf) {
    String deprecated = conf.get(THRIFT_COLUMN_FILTER_KEY);
    String strict = conf.get(STRICT_THRIFT_COLUMN_FILTER_KEY);

    if (Strings.isNullOrEmpty(deprecated) && Strings.isNullOrEmpty(strict)) {
      return null;
    }

    if(!Strings.isNullOrEmpty(deprecated) && !Strings.isNullOrEmpty(strict)) {
      throw new ThriftProjectionException(
          "You cannot provide both "
              + THRIFT_COLUMN_FILTER_KEY
              + " and "
              + STRICT_THRIFT_COLUMN_FILTER_KEY
              +"! "
              + THRIFT_COLUMN_FILTER_KEY
              + " is deprecated."
      );
    }

    if (!Strings.isNullOrEmpty(deprecated)) {
      LOG.warn("Using {} is deprecated. Please see the docs for {}!",
          THRIFT_COLUMN_FILTER_KEY, STRICT_THRIFT_COLUMN_FILTER_KEY);
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
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
    final ParquetConfiguration configuration = context.getParquetConfiguration();
    final MessageType fileMessageType = context.getFileSchema();
    MessageType requestedProjection = fileMessageType;
    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);

    FieldProjectionFilter projectionFilter = getFieldProjectionFilter(configuration);

    if (partialSchemaString != null && projectionFilter != null) {
      throw new ThriftProjectionException(
          String.format("You cannot provide both a partial schema and field projection filter."
                  + "Only one of (%s, %s, %s) should be set.",
              PARQUET_READ_SCHEMA, STRICT_THRIFT_COLUMN_FILTER_KEY, THRIFT_COLUMN_FILTER_KEY));
    }

    //set requestedProjections only when it's specified
    if (partialSchemaString != null) {
      requestedProjection = getSchemaForRead(fileMessageType, partialSchemaString);
    } else if (projectionFilter != null) {
      try {
        initThriftClassFromMultipleFiles(context.getKeyValueMetadata(), configuration);
        requestedProjection =  getProjectedSchema(configuration, projectionFilter);
      } catch (ClassNotFoundException e) {
        throw new ThriftProjectionException("can not find thriftClass from configuration", e);
      }
    }

    MessageType schemaForRead = getSchemaForRead(fileMessageType, requestedProjection);
    return new ReadContext(schemaForRead);
  }

  protected MessageType getProjectedSchema(Configuration configuration, FieldProjectionFilter
      fieldProjectionFilter) {
    return getProjectedSchema(new HadoopParquetConfiguration(configuration), fieldProjectionFilter);
  }

  @SuppressWarnings("unchecked")
  protected MessageType getProjectedSchema(ParquetConfiguration configuration, FieldProjectionFilter
      fieldProjectionFilter) {
    return new ThriftSchemaConverter(configuration, fieldProjectionFilter)
        .convert((Class<TBase<?, ?>>)thriftClass);
  }

  @Deprecated
  @SuppressWarnings("unchecked")
  protected MessageType getProjectedSchema(FieldProjectionFilter
    fieldProjectionFilter) {
    return new ThriftSchemaConverter(new Configuration(), fieldProjectionFilter)
      .convert((Class<TBase<?, ?>>)thriftClass);
  }

  private void initThriftClassFromMultipleFiles(Map<String, Set<String>> fileMetadata, Configuration conf) throws ClassNotFoundException {
    initThriftClassFromMultipleFiles(fileMetadata, new HadoopParquetConfiguration(conf));
  }

  @SuppressWarnings("unchecked")
  private void initThriftClassFromMultipleFiles(Map<String, Set<String>> fileMetadata, ParquetConfiguration conf) throws ClassNotFoundException {
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

  private void initThriftClass(ThriftMetaData metadata, Configuration conf) throws ClassNotFoundException {
    initThriftClass(metadata, new HadoopParquetConfiguration(conf));
  }

  @SuppressWarnings("unchecked")
  private void initThriftClass(ThriftMetaData metadata, ParquetConfiguration conf) throws ClassNotFoundException {
    if (thriftClass != null) {
      return;
    }
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      if (metadata == null) {
        throw new ParquetDecodingException("Could not read file as the Thrift class is not provided and could not be resolved from the file");
      }
      thriftClass = (Class<T>)metadata.getThriftClass();
    } else {
      thriftClass = (Class<T>)Class.forName(className);
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema,
      org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
    try {
      initThriftClass(thriftMetaData, configuration);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find Thrift object class for metadata: " + thriftMetaData, e);
    }

    // if there was not metadata in the file, get it from requested class
    if (thriftMetaData == null) {
      thriftMetaData = ThriftMetaData.fromThriftClass(thriftClass);
    }

    String converterClassName = configuration.get(RECORD_CONVERTER_CLASS_KEY, RECORD_CONVERTER_DEFAULT);
    return getRecordConverterInstance(converterClassName, thriftClass,
        readContext.getRequestedSchema(), thriftMetaData.getDescriptor(),
        configuration);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(ParquetConfiguration configuration,
                                              Map<String, String> keyValueMetaData, MessageType fileSchema,
                                              org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
    try {
      initThriftClass(thriftMetaData, configuration);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find Thrift object class for metadata: " + thriftMetaData, e);
    }

    // if there was not metadata in the file, get it from requested class
    if (thriftMetaData == null) {
      thriftMetaData = ThriftMetaData.fromThriftClass(thriftClass);
    }

    String converterClassName = configuration.get(RECORD_CONVERTER_CLASS_KEY, RECORD_CONVERTER_DEFAULT);
    return getRecordConverterInstance(converterClassName, thriftClass,
      readContext.getRequestedSchema(), thriftMetaData.getDescriptor(),
      configuration);
  }

  private static <T> ThriftRecordConverter<T> getRecordConverterInstance(
      String converterClassName, Class<T> thriftClass,
      MessageType requestedSchema, StructType descriptor, Configuration conf) {
    return getRecordConverterInstance(converterClassName, thriftClass, requestedSchema, descriptor, conf, Configuration.class);
  }

  private static <T> ThriftRecordConverter<T> getRecordConverterInstance(
      String converterClassName, Class<T> thriftClass,
      MessageType requestedSchema, StructType descriptor, ParquetConfiguration conf) {
    return getRecordConverterInstance(converterClassName, thriftClass, requestedSchema, descriptor, conf, ParquetConfiguration.class);
  }

  @SuppressWarnings("unchecked")
  private static <T, CONF> ThriftRecordConverter<T> getRecordConverterInstance(
    String converterClassName, Class<T> thriftClass,
    MessageType requestedSchema, StructType descriptor, CONF conf, Class<CONF> confClass) {

    Class<ThriftRecordConverter<T>> converterClass;
    try {
      converterClass = (Class<ThriftRecordConverter<T>>) Class.forName(converterClassName);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Cannot find Thrift converter class: " + converterClassName, e);
    }

    try {
      // first try the new version that accepts a Configuration
      try {
        Constructor<ThriftRecordConverter<T>> constructor =
          converterClass.getConstructor(Class.class, MessageType.class, StructType.class, confClass);
        return constructor.newInstance(thriftClass, requestedSchema, descriptor, conf);
      } catch (IllegalAccessException | NoSuchMethodException e) {
        // try the other constructor pattern
      }

      Constructor<ThriftRecordConverter<T>> constructor =
        converterClass.getConstructor(Class.class, MessageType.class, StructType.class);
      return constructor.newInstance(thriftClass, requestedSchema, descriptor);
    } catch (InstantiationException | InvocationTargetException e) {
      throw new RuntimeException("Failed to construct Thrift converter class: " + converterClassName, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Cannot access constructor for Thrift converter class: " + converterClassName, e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Cannot find constructor for Thrift converter class: " + converterClassName, e);
    }
  }
}
