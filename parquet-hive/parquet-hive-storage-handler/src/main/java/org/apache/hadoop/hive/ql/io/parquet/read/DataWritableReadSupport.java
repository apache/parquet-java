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
package org.apache.hadoop.hive.ql.io.parquet.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.convert.DataWritableRecordConverter;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hive.HiveBindingFactory;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

/**
 *
 * A MapWritableReadSupport
 *
 * Manages the translation between Hive and Parquet
 *
 */
public class DataWritableReadSupport extends ReadSupport<ArrayWritable> {

  private static final String TABLE_SCHEMA = "table_schema";
  public static final String HIVE_SCHEMA_KEY = "HIVE_TABLE_SCHEMA";
  public static final String PARQUET_COLUMN_INDEX_ACCESS = "parquet.column.index.access";

  /**
   * From a string which columns names (including hive column), return a list
   * of string columns
   *
   * @param comma separated list of columns
   * @return list with virtual columns removed
   */
  private static List<String> getColumns(final String columns) {
    return (new HiveBindingFactory()).create().getColumns(columns);
  }
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
  public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(final Configuration configuration,
      final Map<String, String> keyValueMetaData, final MessageType fileSchema) {
    final String columns = configuration.get(IOConstants.COLUMNS);
    final Map<String, String> contextMetadata = new HashMap<String, String>();
    if (columns != null) {
      final List<String> listColumns = getColumns(columns);

      final List<Type> typeListTable = new ArrayList<Type>();
      for (final String col : listColumns) {
        // listColumns contains partition columns which are metadata only
        if (fileSchema.containsField(col)) {
          typeListTable.add(fileSchema.getType(col));
        } else {
          // below allows schema evolution
          typeListTable.add(new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, col));
        }
      }
      MessageType tableSchema = new MessageType(TABLE_SCHEMA, typeListTable);
      contextMetadata.put(HIVE_SCHEMA_KEY, tableSchema.toString());

      MessageType requestedSchemaByUser = tableSchema;
      final List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(configuration);

      final List<Type> typeListWanted = new ArrayList<Type>();
      for (final Integer idx : indexColumnsWanted) {
        typeListWanted.add(tableSchema.getType(listColumns.get(idx)));
      }
      requestedSchemaByUser = resolveSchemaAccess(new MessageType(fileSchema.getName(),
              typeListWanted), fileSchema, configuration);

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
  public RecordMaterializer<ArrayWritable> prepareForRead(final Configuration configuration,
      final Map<String, String> keyValueMetaData, final MessageType fileSchema,
          final org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext) {
    final Map<String, String> metadata = readContext.getReadSupportMetadata();
    if (metadata == null) {
      throw new IllegalStateException("ReadContext not initialized properly. " +
        "Don't know the Hive Schema.");
    }
    final MessageType tableSchema = resolveSchemaAccess(MessageTypeParser.
        parseMessageType(metadata.get(HIVE_SCHEMA_KEY)), fileSchema, configuration);

    return new DataWritableRecordConverter(readContext.getRequestedSchema(), tableSchema);
  }

  /**
  * Determine the file column names based on the position within the requested columns and
  * use that as the requested schema.
  */
  private MessageType resolveSchemaAccess(MessageType requestedSchema, MessageType fileSchema,
          Configuration configuration) {
    if(configuration.getBoolean(PARQUET_COLUMN_INDEX_ACCESS, false)) {
      final List<String> listColumns = getColumns(configuration.get(IOConstants.COLUMNS));

      List<Type> requestedTypes = new ArrayList<Type>();

      for(Type t : requestedSchema.getFields()) {
        int index = listColumns.indexOf(t.getName());
        requestedTypes.add(fileSchema.getType(index));
      }

      requestedSchema = new MessageType(requestedSchema.getName(), requestedTypes);
    }

    return requestedSchema;
  }
}
