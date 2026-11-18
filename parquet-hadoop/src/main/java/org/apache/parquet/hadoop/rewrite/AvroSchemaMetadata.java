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
package org.apache.parquet.hadoop.rewrite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Keeps the Avro schema stored in the file metadata by parquet-avro consistent with the Parquet schema of a rewritten
 * file, when columns are pruned or renamed.
 * <p>
 * parquet-avro reads files with the Avro schema stored in the key-value metadata, if present. If that schema still
 * declares a pruned field, readers return nulls for it - even if the field is required. Therefore, the fields that are
 * no longer present in the Parquet schema are removed from the Avro schema, and renamed top-level fields are renamed.
 * If the Avro schema can't be adjusted reliably (for example, if a pruned record type is reused by name elsewhere),
 * it is removed from the metadata, so that parquet-avro derives the Avro schema from the Parquet schema.
 * <p>
 * This class does not depend on Avro: the schema is processed as JSON, following the Avro to Parquet schema mapping
 * of parquet-avro's {@code AvroSchemaConverter}.
 */
final class AvroSchemaMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaMetadata.class);

  /** Metadata keys of the Avro schema, as written by parquet-avro (current and legacy). */
  static final List<String> AVRO_SCHEMA_KEYS = Arrays.asList("parquet.avro.schema", "avro.schema");

  private static final Set<String> PRIMITIVE_TYPES =
      new HashSet<>(Arrays.asList("null", "boolean", "int", "long", "float", "double", "bytes", "string"));

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private AvroSchemaMetadata() {}

  /**
   * @param keyValueMetaData the key-value metadata of the output file
   * @param schema the Parquet schema of the output file, after pruning and before renaming
   * @param renamedColumns renamed top-level fields: original names to new names
   * @return the key-value metadata, with Avro schemas consistent with the Parquet schema
   */
  static Map<String, String> adjust(
      Map<String, String> keyValueMetaData, MessageType schema, Map<String, String> renamedColumns) {
    Map<String, String> result = new HashMap<>(keyValueMetaData);
    for (String key : AVRO_SCHEMA_KEYS) {
      String avroSchema = keyValueMetaData.get(key);
      if (avroSchema == null) {
        continue;
      }
      try {
        result.put(key, adjust(avroSchema, schema, renamedColumns));
      } catch (InconsistentSchemaException | JsonProcessingException e) {
        LOG.warn(
            "Removing the Avro schema '{}' from the metadata, because it can't be adjusted to the rewritten "
                + "Parquet schema: {}. Avro readers will derive the Avro schema from the Parquet schema.",
            key,
            e.getMessage());
        result.remove(key);
      }
    }
    return result;
  }

  static String adjust(String avroSchema, MessageType schema, Map<String, String> renamedColumns)
      throws JsonProcessingException, InconsistentSchemaException {
    JsonNode root = MAPPER.readTree(avroSchema);
    if (!root.isObject() || !isRecord(root)) {
      throw new InconsistentSchemaException("the Avro schema is not a record");
    }
    Set<String> recordNames = new HashSet<>();
    Set<String> namedReferences = new HashSet<>();
    adjustRecord((ObjectNode) root, schema, recordNames, namedReferences);
    for (String reference : namedReferences) {
      if (recordNames.contains(shortName(reference))) {
        // The same record definition is shared by several fields, that may have been pruned differently
        throw new InconsistentSchemaException("the record type '" + reference + "' is reused by name");
      }
    }
    renameFields((ObjectNode) root, renamedColumns);
    return MAPPER.writeValueAsString(root);
  }

  private static void adjustRecord(
      ObjectNode record, Type parquetType, Set<String> recordNames, Set<String> namedReferences)
      throws InconsistentSchemaException {
    if (parquetType.isPrimitive()) {
      throw new InconsistentSchemaException(
          "the record '" + record.path("name").asText() + "' is not a group in the Parquet schema");
    }
    GroupType group = parquetType.asGroupType();
    addRecordNames(record, recordNames);
    JsonNode fields = record.get("fields");
    if (fields == null || !fields.isArray()) {
      throw new InconsistentSchemaException(
          "the record '" + record.path("name").asText() + "' has no fields");
    }
    ArrayNode fieldArray = (ArrayNode) fields;
    Set<String> avroFieldNames = new HashSet<>();
    for (int i = fieldArray.size() - 1; i >= 0; i--) {
      JsonNode field = fieldArray.get(i);
      String name = field.path("name").asText();
      if (!group.containsField(name)) {
        // Pruned field
        fieldArray.remove(i);
        continue;
      }
      avroFieldNames.add(name);
      adjustType(field.get("type"), group.getType(name), recordNames, namedReferences);
    }
    for (Type parquetField : group.getFields()) {
      if (!avroFieldNames.contains(parquetField.getName())) {
        throw new InconsistentSchemaException(
            "the Parquet field '" + parquetField.getName() + "' is missing in the Avro record '"
                + record.path("name").asText() + "'");
      }
    }
  }

  private static void adjustType(
      JsonNode avroType, Type parquetType, Set<String> recordNames, Set<String> namedReferences)
      throws InconsistentSchemaException {
    if (avroType == null) {
      throw new InconsistentSchemaException(
          "missing Avro type for the Parquet field '" + parquetType.getName() + "'");
    }
    if (avroType.isTextual()) {
      if (!PRIMITIVE_TYPES.contains(avroType.asText())) {
        namedReferences.add(avroType.asText());
      }
      return;
    }
    if (avroType.isArray()) {
      adjustUnion((ArrayNode) avroType, parquetType, recordNames, namedReferences);
      return;
    }
    if (!avroType.isObject() || !avroType.path("type").isTextual()) {
      throw new InconsistentSchemaException(
          "unsupported Avro type for the Parquet field '" + parquetType.getName() + "': " + avroType);
    }
    switch (avroType.get("type").asText()) {
      case "record":
      case "error":
        adjustRecord((ObjectNode) avroType, parquetType, recordNames, namedReferences);
        break;
      case "array":
        adjustType(avroType.get("items"), listElement(parquetType), recordNames, namedReferences);
        break;
      case "map":
        adjustType(avroType.get("values"), mapValue(parquetType), recordNames, namedReferences);
        break;
      default:
        // Primitive types (possibly with a logical type), enums and fixed: nothing to prune
        break;
    }
  }

  /**
   * Unions with a single non-null type are optional Parquet fields; other unions are groups with a
   * {@code memberN} field for each non-null type.
   */
  private static void adjustUnion(
      ArrayNode union, Type parquetType, Set<String> recordNames, Set<String> namedReferences)
      throws InconsistentSchemaException {
    int nonNullTypes = 0;
    for (JsonNode branch : union) {
      if (!(branch.isTextual() && "null".equals(branch.asText()))) {
        nonNullTypes++;
      }
    }
    if (nonNullTypes <= 1) {
      for (JsonNode branch : union) {
        if (!(branch.isTextual() && "null".equals(branch.asText()))) {
          adjustType(branch, parquetType, recordNames, namedReferences);
        }
      }
      return;
    }
    if (parquetType.isPrimitive() || parquetType.asGroupType().getFieldCount() != nonNullTypes) {
      throw new InconsistentSchemaException(
          "the union field '" + parquetType.getName() + "' does not match the Parquet schema");
    }
    GroupType group = parquetType.asGroupType();
    int index = 0;
    for (JsonNode branch : union) {
      if (branch.isTextual() && "null".equals(branch.asText())) {
        continue;
      }
      String member = "member" + index++;
      if (!group.containsField(member)) {
        throw new InconsistentSchemaException("the union field '" + parquetType.getName() + "' has no '"
            + member + "' in the Parquet schema");
      }
      adjustType(branch, group.getType(member), recordNames, namedReferences);
    }
  }

  /** The element of a 2-level ({@code repeated ... array}) or 3-level ({@code list.element}) Parquet list. */
  private static Type listElement(Type parquetType) throws InconsistentSchemaException {
    if (!parquetType.isPrimitive() && parquetType.asGroupType().getFieldCount() == 1) {
      Type repeated = parquetType.asGroupType().getType(0);
      if (repeated.isRepetition(Type.Repetition.REPEATED)) {
        if ("array".equals(repeated.getName())) {
          return repeated;
        }
        if ("list".equals(repeated.getName())
            && !repeated.isPrimitive()
            && repeated.asGroupType().getFieldCount() == 1
            && repeated.asGroupType().containsField("element")) {
          return repeated.asGroupType().getType("element");
        }
      }
    }
    throw new InconsistentSchemaException(
        "the array field '" + parquetType.getName() + "' does not match the Parquet schema");
  }

  /** The value of a Parquet map ({@code key_value.value}). */
  private static Type mapValue(Type parquetType) throws InconsistentSchemaException {
    if (!parquetType.isPrimitive() && parquetType.asGroupType().getFieldCount() == 1) {
      Type repeated = parquetType.asGroupType().getType(0);
      if (repeated.isRepetition(Type.Repetition.REPEATED)
          && !repeated.isPrimitive()
          && repeated.asGroupType().getFieldCount() == 2
          && repeated.asGroupType().containsField("key")
          && repeated.asGroupType().containsField("value")) {
        return repeated.asGroupType().getType("value");
      }
    }
    throw new InconsistentSchemaException(
        "the map field '" + parquetType.getName() + "' does not match the Parquet schema");
  }

  private static void renameFields(ObjectNode record, Map<String, String> renamedColumns) {
    if (renamedColumns == null || renamedColumns.isEmpty()) {
      return;
    }
    for (JsonNode field : record.get("fields")) {
      String newName = renamedColumns.get(field.path("name").asText());
      if (newName != null) {
        ((ObjectNode) field).put("name", newName);
      }
    }
  }

  private static boolean isRecord(JsonNode avroType) {
    String type = avroType.path("type").asText();
    return "record".equals(type) || "error".equals(type);
  }

  /**
   * Record names are compared without their namespace, which may be inherited from an enclosing type: a match is
   * either a reuse of the record, or a different record with the same name, which also leads to removing the schema.
   */
  private static void addRecordNames(ObjectNode record, Set<String> recordNames) {
    recordNames.add(shortName(record.path("name").asText()));
  }

  private static String shortName(String name) {
    return name.substring(name.lastIndexOf('.') + 1);
  }

  static final class InconsistentSchemaException extends Exception {
    InconsistentSchemaException(String message) {
      super(message);
    }
  }
}
