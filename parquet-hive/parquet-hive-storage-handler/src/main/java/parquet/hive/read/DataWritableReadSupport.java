/**
 * Copyright 2013 Criteo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package parquet.hive.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;

import parquet.hadoop.api.ReadSupport;
import parquet.hive.HiveBindingFactory;
import parquet.hive.convert.DataWritableRecordConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;
import parquet.schema.Type.Repetition;

/**
 *
 * A MapWritableReadSupport
 *
 * Manages the translation between Hive and Parquet
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class DataWritableReadSupport extends ReadSupport<ArrayWritable> {

  public static final String HIVE_SCHEMA_KEY = "HIVE_TABLE_SCHEMA";

  /**
   *
   * It creates the readContext for Parquet side with the requested schema during the init phase.
   *
   * @param configuration needed to get the wanted columns
   * @param keyValueMetaData // unused
   * @param fileSchema parquet file schema
   * @return the parquet ReadContext
   */
  @Override
  public parquet.hadoop.api.ReadSupport.ReadContext init(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    final String columns = configuration.get("columns");
    final Map<String, String> contextMetadata = new HashMap<String, String>();
    if (columns != null) {
      final List<String> listColumns = (new HiveBindingFactory()).create().getColumns(columns);

      final List<Type> typeListTable = new ArrayList<Type>();
      for (final String col : listColumns) {
        if (fileSchema.containsField(col)) {
          typeListTable.add(fileSchema.getType(col));
        } else { // dummy type, should not be called
          typeListTable.add(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, col));
        }
      }
      MessageType tableSchema = new MessageType("table_schema", typeListTable);
      contextMetadata.put(HIVE_SCHEMA_KEY, tableSchema.toString());

      MessageType requestedSchemaByUser = tableSchema;
      final List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);

      final List<Type> typeListWanted = new ArrayList<Type>();
      for (final Integer idx : indexColumnsWanted) {
        typeListWanted.add(tableSchema.getType(listColumns.get(idx)));
      }
      requestedSchemaByUser = new MessageType(fileSchema.getName(), typeListWanted);

      return new ReadContext(requestedSchemaByUser, contextMetadata);
    } else {
      contextMetadata.put(HIVE_SCHEMA_KEY, fileSchema.toString());
      return new ReadContext(fileSchema, contextMetadata);
    }
  }

  /**
   *
   * It creates the hive read support to interpret data from parquet to hive
   *
   * @param configuration // unused
   * @param keyValueMetaData
   * @param fileSchema // unused
   * @param readContext containing the requested schema and the schema of the hive table
   * @return Record Materialize for Hive
   */
  @Override
  public RecordMaterializer<ArrayWritable> prepareForRead(final Configuration configuration, final Map<String, String> keyValueMetaData, final MessageType fileSchema,
          final parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    final Map<String, String> metadata = readContext.getReadSupportMetadata();
    if (metadata == null) {
      throw new RuntimeException("ReadContext not initialized properly. Don't know the Hive Schema.");
    }
    final MessageType tableSchema = MessageTypeParser.parseMessageType(metadata.get(HIVE_SCHEMA_KEY));
    return new DataWritableRecordConverter(readContext.getRequestedSchema(), tableSchema);
  }
}
