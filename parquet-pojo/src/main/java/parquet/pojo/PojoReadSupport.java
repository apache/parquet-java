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
import parquet.hadoop.api.InitContext;
import parquet.hadoop.api.ReadSupport;
import parquet.io.api.RecordMaterializer;
import parquet.pojo.field.FieldUtils;
import parquet.schema.MessageType;

import java.util.Map;

/**
 * ReadSupport for reading native java objects from a parquet file.
 */
public class PojoReadSupport extends ReadSupport<Object> {
  private String getFromConfOrFallbackToMetadata(
    String key, Configuration configuration, Map<String, String> metadata
  ) {
    String result = configuration.get(key);

    if (result == null) {
      return metadata.get(key);
    }

    return result;
  }

  @Override
  public ReadContext init(InitContext context) {
    return new ReadContext(context.getFileSchema(), context.getMergedKeyValueMetaData());
  }

  public RecordMaterializer prepareForRead(
    Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema,
    ReadContext readContext
  ) {
    String className = getFromConfOrFallbackToMetadata(
      ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY, configuration, keyValueMetaData
    );

    if (className == null) {
      throw new BadConfigurationException(
        String.format(
          "%s not set on the output format.",
          ParquetPojoConstants.PARQUET_POJO_INPUT_CLASS_KEY
        )
      );
    }

    try {
      Class encodedClass = PojoUtils.classForNameWithPrimitiveSupport(className);

      Class[] genericArguments = new Class[0];

      if (FieldUtils.isMap(encodedClass)) {
        genericArguments = new Class[]{
          PojoUtils.classForNameWithPrimitiveSupport(
            getFromConfOrFallbackToMetadata(
              ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_KEY_CLASS, configuration, keyValueMetaData
            )
          ),
          PojoUtils.classForNameWithPrimitiveSupport(
            getFromConfOrFallbackToMetadata(
              ParquetPojoConstants.PARQUET_POJO_OUTPUT_MAP_VALUE_CLASS, configuration, keyValueMetaData
            )
          )
        };
      } else if (FieldUtils.isList(encodedClass)) {
        genericArguments = new Class[]{
          PojoUtils.classForNameWithPrimitiveSupport(
            getFromConfOrFallbackToMetadata(
              ParquetPojoConstants.PARQUET_POJO_OUTPUT_LIST_VALUE_CLASS, configuration, keyValueMetaData
            )
          )
        };
      }

      return new PojoRecordMaterializer(encodedClass, genericArguments);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
