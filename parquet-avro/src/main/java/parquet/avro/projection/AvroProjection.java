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
package parquet.avro.projection;

import org.apache.avro.Schema;
import  org.apache.avro.Schema.Type.*;

import java.util.*;

// originally based on https://raw.githubusercontent.com/epishkin/scalding/parquet_avro/scalding-parquet/src/main/scala/com/twitter/scalding/parquet/avro/Projection.scala

/**
 * Helps create projections for Avro schemas.
 */
public class AvroProjection {
    /**
     * Create a projection for {{schema}} that includes {{fields}}.
     */
    public static Schema createProjection(Schema schema, String[] fields)  {
        return createProjection(schema, fields, null);
    }


    private static Schema createProjection(Schema schema, String[] fields, String parentFieldName) {
        switch(schema.getType()) {
            case RECORD:
                return createRecordProjection(schema, fields, parentFieldName);
            case UNION:
                return createUnionProjection(schema, fields, parentFieldName);
            case ARRAY:
                return createArrayProjection(schema, fields, parentFieldName);
            default:
                String fieldInfo = schema.getType().toString();
                if (parentFieldName != null) {
                    fieldInfo = parentFieldName + fieldInfo;
                }
                String children = convertFieldNamesToString(fields);
                throw new RuntimeException("Projection doesn't support schema type " + fieldInfo + " with fields: " + children);
        }
    }

    private static Schema createRecordProjection(Schema schema, String[] fields, String parentFieldName) {
        // Take the head of any nested properties, "parent.fieldX" => "parent"
        List<String> nestedFields = new LinkedList<String>();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].contains(".")) {
                nestedFields.add(fields[i].split("\\.")[0]);
            }
        }
        List<String> directFields = new LinkedList<String>();
        Collections.copy(directFields, nestedFields);
        directFields.addAll(Arrays.asList(fields));

        List<Schema.Field> schemaFields = schema.getFields();
        validateRequestedFields(schema, schemaFields, directFields);

        List<Schema.Field> projectedFields = new LinkedList<Schema.Field>();

        for (Schema.Field field : schemaFields) {
            if (directFields.contains(field.name())) {
                Schema thisSchema;
                // Create projection for the nested field
                if (nestedFields.contains(field.name())) {
                    String prefix = field.name() + ".";
                    // Find the nested fields and remove the prefix
                    List<String> children = new LinkedList<String>();
                    for (String requestedField : fields) {
                        if (requestedField.startsWith(prefix)) {
                            children.add(requestedField.substring(0, prefix.length()));
                        }
                    }
                    thisSchema = createProjection(field.schema(), (String[])children.toArray(), fullFieldName(parentFieldName, field.name()));
                } else {
                    thisSchema = field.schema();
                }

                projectedFields.add(copyField(thisSchema, field));
            }
        }

        Schema projection = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
        projection.setFields(projectedFields);
        return projection;
    }

    private static Schema createUnionProjection(Schema schema, String[] fields, String parentFieldName) {
        List<Schema> projectedSchemas = new LinkedList<Schema>();
        for (Schema nestedSchema : schema.getTypes()) {
            if (nestedSchema.getType() == Schema.Type.NULL) {
                projectedSchemas.add(nestedSchema);
            } else {
                projectedSchemas.add(createProjection(nestedSchema, fields, parentFieldName));
            }
        }
        return Schema.createUnion(projectedSchemas);
    }

    private static Schema createArrayProjection(Schema schema, String[] fields, String parentFieldName) {
        return Schema.createArray(
                createProjection(schema.getElementType(), fields, parentFieldName));
    }

    private static void validateRequestedFields(Schema schema, List<Schema.Field> schemaFields, List<String> fieldNames) throws RuntimeException {
        Set<String> schemaFieldNames = new HashSet<String>();
        for (Schema.Field field: schemaFields) {
            schemaFieldNames.add(field.name());
        }
        for (String field: fieldNames) {
            if (!field.contains("\\.") && !schemaFieldNames.contains(field)) {
                String full_name = schema.getFullName();
                final String supported_fields = Arrays.toString(schemaFieldNames.toArray());
                throw new RuntimeException(
                        "Field " + field + " not found in schema " + full_name + ". " +
                                "Supported fields are: " + supported_fields
                );
            }
        }
    }

    private static Schema.Field copyField(Schema schema, Schema.Field field) {
        return (new AvroCustomField(schema, field));
    }

    private static String fullFieldName(String parentFieldName, String fieldName) {
        if (parentFieldName == null) {
            return null;
        } else {
            return (parentFieldName + "." + fieldName);
        }
    }

    private static String convertFieldNamesToString(String[] fields) {
        StringBuilder sb = new StringBuilder();
        for (String field : fields) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(field);
        }
        return sb.toString();
    }

}
