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
package parquet.pojo;

import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.BadConfigurationException;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.RecordConsumer;
import parquet.pojo.field.FieldUtils;
import parquet.pojo.writer.RootWriter;
import parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

public class PojoWriteSupport extends WriteSupport {
  private RecordConsumer recordConsumer;
  private RootWriter rootWriter;

  @Override
  public WriteContext init(Configuration configuration) {
    String className = configuration.get(ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY);

    if (className == null) {
      throw new BadConfigurationException(
        String.format(
          "No value found in configuration for property %s. Check that a schema class was specified",
          ParquetPojoConstants.PARQUET_POJO_OUTPUT_CLASS_KEY
        )
      );
    }

    try {
      Class clazz = PojoUtils.classForNameWithPrimitiveSupport(className);
      Map<String, String> keyValueMetadata = new HashMap<String, String>();
      keyValueMetadata.put(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, className);
      Class[] genericArguments = new Class[0];

      if (FieldUtils.isMap(clazz)) {
        String mapKeyClassName = configuration.get(ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_KEY_CLASS);
        String mapValueClassName = configuration.get(ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_VALUE_CLASS);

        keyValueMetadata.put(ParquetPojoConstants.PARQUET_POJO_INPUT_MAP_KEY_CLASS, mapKeyClassName);
        keyValueMetadata.put(ParquetPojoConstants.PARQUET_POJO_INPUT_MAP_VALUE_CLASS, mapValueClassName);

        genericArguments = new Class[]{
          PojoUtils.classForNameWithPrimitiveSupport(
            mapKeyClassName
          ), PojoUtils.classForNameWithPrimitiveSupport(
          mapValueClassName
        )};
      } else if (FieldUtils.isList(clazz)) {
        String listValueClassName = configuration.get(ParquetPojoConstants.PARQUET_POJO_OUTPUT_LIST_VALUE_CLASS);

        keyValueMetadata.put(ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, className);
        keyValueMetadata.put(ParquetPojoConstants.PARQUET_POJO_INPUT_LIST_VALUE_CLASS, listValueClassName);

        genericArguments = new Class[]{
          PojoUtils.classForNameWithPrimitiveSupport(
            listValueClassName
          )
        };
      }

      rootWriter = new RootWriter(clazz, genericArguments);
      MessageType messageType = new PojoSchemaConverter().convert(clazz, null, genericArguments);

      return new WriteContext(messageType, keyValueMetadata);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @Override
  public void write(Object record) {
    if (record == null) {
      return;
    }

    recordConsumer.startMessage();
    rootWriter.write(record, recordConsumer);
    recordConsumer.endMessage();
  }
}