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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.thrift.TBaseRecordConverter;
import parquet.thrift.ThriftMetaData;
import parquet.thrift.ThriftRecordConverter;

public class ThriftReadSupport<T> extends ReadSupport<T> {

  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(
      Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema) {
    // TODO: handle the requested schema
    return new ReadContext(fileSchema);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(Configuration configuration,
      Map<String, String> keyValueMetaData, MessageType fileSchema,
      parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    final ThriftMetaData thriftMetaData = ThriftMetaData.fromExtraMetaData(keyValueMetaData);
    final Class<T> thriftClass = (Class<T>)thriftMetaData.getThriftClass();
    // TODO: handle the requested schema
    ThriftRecordConverter<T> converter = new TBaseRecordConverter(thriftClass, fileSchema, thriftMetaData.getDescriptor());
    return converter;
  }


}
