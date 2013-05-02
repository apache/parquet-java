/**
 * Copyright 2013 Criteo.
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
package parquet.hive.read;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.util.StringUtils;

import parquet.hadoop.api.ReadSupport;
import parquet.hive.convert.MapWritableRecordConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.Type;

/**
 *
 * A MapWritableReadSupport
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class MapWritableReadSupport extends ReadSupport<MapWritable> {

  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    final List<String> listColumns = (List<String>) StringUtils.getStringCollection(configuration.get("columns"));

    MessageType requestedSchemaByUser = fileSchema;
    final List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);

    if (indexColumnsWanted.isEmpty() == false) {
      final List<Type> typeList = new ArrayList<Type>();
      for (final Integer idx : indexColumnsWanted) {
        typeList.add(fileSchema.getType(listColumns.get(idx)));
      }
      requestedSchemaByUser = new MessageType(fileSchema.getName(), typeList);
    }

    return new ReadContext(requestedSchemaByUser);
  }

  @Override
  public RecordMaterializer<MapWritable> prepareForRead(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema,
      final parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    return new MapWritableRecordConverter(readContext.getRequestedSchema(), keyValueMetaData, fileSchema);
  }
}
