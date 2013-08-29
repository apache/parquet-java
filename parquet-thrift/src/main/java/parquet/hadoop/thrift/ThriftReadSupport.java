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
package parquet.hadoop.thrift;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocol;

import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.thrift.TBaseRecordConverter;
import parquet.thrift.ThriftMetaData;
import parquet.thrift.ThriftRecordConverter;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.projection.FieldProjectionFilter;
import parquet.thrift.projection.ThriftProjectionException;
import parquet.thrift.struct.ThriftType.StructType;

public class ThriftReadSupport<T extends TBase<?,?>> extends ReadSupport<T> {
  /**
   * configuration key for thrift read projection schema
   */
  public static final String THRIFT_READ_FILTER = "thrift.read.filter";
  private static final String RECORD_CONVERTER_DEFAULT = TBaseRecordConverter.class.getName();
  public static final String THRIFT_READ_CLASS_KEY = "parquet.thrift.read.class";

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified using this key
   * (for example, ScroogeRecordConverter from parquet-scrooge).
   */
  private static final String RECORD_CONVERTER_CLASS_KEY = "parquet.thrift.converter.class";

  private Class<T> thriftClass;

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
  public parquet.hadoop.api.ReadSupport.ReadContext init(
          Configuration configuration, Map<String, String> keyValueMetaData,
          MessageType fileMessageType) {
    MessageType requestedProjection;
    String partialSchemaString = configuration.get(ReadSupport.PARQUET_READ_SCHEMA);
    String projectionSchemaStr = configuration.get(THRIFT_READ_FILTER);

    if (partialSchemaString != null && projectionSchemaStr != null)
      throw new ThriftProjectionException("PARQUET_READ_SCHEMA and THRIFT_READ_FILTER are both specified, should use only one.");

    if (partialSchemaString != null) {
      requestedProjection = getSchemaForRead(fileMessageType, partialSchemaString);
    } else {
      FieldProjectionFilter fieldProjectionFilter = new FieldProjectionFilter(projectionSchemaStr);
      ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
      try {
        requestedProjection = new ThriftSchemaConverter(fieldProjectionFilter).convert(getThriftClass(thriftMetaData, configuration));
      } catch (ClassNotFoundException e) {
        throw new ThriftProjectionException("can not find thriftClass from configuration");
      }
    }

    MessageType schemaForRead = getSchemaForRead(fileMessageType, requestedProjection);
    return new ReadContext(schemaForRead);
  }

  @SuppressWarnings("unchecked")
  private Class<T> getThriftClass(ThriftMetaData metadata, Configuration conf) throws ClassNotFoundException {
    if (thriftClass != null) {
      return thriftClass;
    }
    String className = conf.get(THRIFT_READ_CLASS_KEY, null);
    if (className == null) {
      return (Class<T>) metadata.getThriftClass();
    } else {
        return (Class<T>) Class.forName(className);
    }
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema,
      parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    final ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
    try {
      final Class<T> thriftClass = getThriftClass(thriftMetaData, configuration);

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
