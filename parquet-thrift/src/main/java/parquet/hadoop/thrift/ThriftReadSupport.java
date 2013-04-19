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
import parquet.thrift.struct.ThriftType.StructType;

public class ThriftReadSupport<T> extends ReadSupport<T> {
  private static final String RECORD_CONVERTER_DEFAULT = TBaseRecordConverter.class.getName();
  public static final String THRIFT_READ_CLASS_KEY = "parquet.thrift.read.class";

  /**
   * A {@link ThriftRecordConverter} builds an object by working with {@link TProtocol}. The default
   * implementation creates standard Apache Thrift {@link TBase} objects; to support alternatives, such
   * as <a href="http://github.com/twitter/scrooge">Twiter's Scrooge</a>, a custom converter can be specified using this key
   * (for example, ScroogeRecordConverter from parquet-scrooge).
   */
  private static final String RECORD_CONVERTER_CLASS_KEY = "parquet.thrift.converter.class";

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


  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema) {
    // TODO: handle the requested schema
    return new ReadContext(fileSchema);
  }

  @SuppressWarnings("unchecked")
  private Class<T> getThriftClass(ThriftMetaData metadata, Configuration conf) throws ClassNotFoundException {
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
      // TODO: handle the requested schema
      ThriftRecordConverter<T> converter = constructor.newInstance(thriftClass, fileSchema, thriftMetaData.getDescriptor());
      return converter;
    } catch (Exception t) {
      throw new RuntimeException("Unable to create Thrift Converter: ", t);
    }
  }

}
