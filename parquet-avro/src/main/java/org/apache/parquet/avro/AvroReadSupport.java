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
package org.apache.parquet.avro;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Avro implementation of {@link ReadSupport} for avro generic, specific, and
 * reflect models. Use {@link AvroParquetReader} or
 * {@link AvroParquetInputFormat} rather than using this class directly.
 *
 * @param <T> the Java type of records created by this ReadSupport
 */
public class AvroReadSupport<T> extends ReadSupport<T> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroReadSupport.class);

  public static final String AVRO_REQUESTED_PROJECTION = "parquet.avro.projection";
  private static final String AVRO_READ_SCHEMA = "parquet.avro.read.schema";

  static final String AVRO_SCHEMA_METADATA_KEY = "parquet.avro.schema";
  // older files were written with the schema in this metadata key
  static final String OLD_AVRO_SCHEMA_METADATA_KEY = "avro.schema";
  private static final String AVRO_READ_SCHEMA_METADATA_KEY = "avro.read.schema";

  public static final String AVRO_DATA_SUPPLIER = "parquet.avro.data.supplier";

  public static final String AVRO_COMPATIBILITY = "parquet.avro.compatible";
  public static final boolean AVRO_DEFAULT_COMPATIBILITY = true;

  // Support reading Parquet INT96 as a 12-byte array.
  public static final String READ_INT96_AS_FIXED = "parquet.avro.readInt96AsFixed";
  public static final boolean READ_INT96_AS_FIXED_DEFAULT = false;

  // Automatically detect whether a Parquet file uses 2-level or 3-level encoding;
  // Ignored if AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE is also set
  public static final String AUTO_DETECT_LIST_STRUCTURE = "parquet.avro.read.autoDetectListStructure";
  static final boolean AUTO_DETECT_LIST_STRUCTURE_DEFAULT = true;

  /**
   * List of the fully qualified class names separated by ',' that may be referenced from the Avro schema by
   * "java-class" or "java-key-class" and are allowed to be loaded.
   */
  public static final String SERIALIZABLE_CLASSES = "parquet.avro.serializable.classes";

  /**
   * @param configuration       a configuration
   * @param requestedProjection the requested projection schema
   * @see org.apache.parquet.avro.AvroParquetInputFormat#setRequestedProjection(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setRequestedProjection(Configuration configuration, Schema requestedProjection) {
    configuration.set(AVRO_REQUESTED_PROJECTION, requestedProjection.toString());
  }

  /**
   * @param configuration  a configuration
   * @param avroReadSchema the read schema
   * @see org.apache.parquet.avro.AvroParquetInputFormat#setAvroReadSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setAvroReadSchema(Configuration configuration, Schema avroReadSchema) {
    configuration.set(AVRO_READ_SCHEMA, avroReadSchema.toString());
  }

  public static void setAvroDataSupplier(Configuration configuration, Class<? extends AvroDataSupplier> clazz) {
    configuration.set(AVRO_DATA_SUPPLIER, clazz.getName());
  }

  public static void setSerializableClasses(Configuration configuration, String... classNames) {
    if (classNames.length == 0) {
      configuration.set(AvroReadSupport.SERIALIZABLE_CLASSES, null);
    } else {
      configuration.set(AvroReadSupport.SERIALIZABLE_CLASSES, String.join(",", classNames));
    }
  }

  public static void setSerializableClasses(Configuration configuration, Class<?>... classes) {
    if (classes.length == 0) {
      configuration.set(AvroReadSupport.SERIALIZABLE_CLASSES, null);
    } else {
      configuration.set(
          AvroReadSupport.SERIALIZABLE_CLASSES,
          Stream.of(classes).map(Class::getName).collect(Collectors.joining(",")));
    }
  }

  private GenericData model = null;

  public AvroReadSupport() {}

  public AvroReadSupport(GenericData model) {
    this.model = model;
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    return init(new HadoopParquetConfiguration(configuration), keyValueMetaData, fileSchema);
  }

  @Override
  public ReadContext init(
      ParquetConfiguration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    MessageType projection = fileSchema;
    Map<String, String> metadata = new LinkedHashMap<String, String>();

    String requestedProjectionString = configuration.get(AVRO_REQUESTED_PROJECTION);
    if (requestedProjectionString != null) {
      Schema avroRequestedProjection = new Schema.Parser().parse(requestedProjectionString);
      ParquetConfiguration listEncodingConf = getDerivedListEncodingConf(configuration, fileSchema);
      projection = new AvroSchemaConverter(listEncodingConf).convert(avroRequestedProjection);
      boolean oldListStructure = listEncodingConf.getBoolean(
          AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT);
      projection = new MessageType(
          projection.getName(),
          alignListStructures(
              projection,
              fileSchema,
              avroRequestedProjection,
              materializedSchema(configuration, keyValueMetaData),
              oldListStructure));
    }

    String avroReadSchema = configuration.get(AVRO_READ_SCHEMA);
    if (avroReadSchema != null) {
      metadata.put(AVRO_READ_SCHEMA_METADATA_KEY, avroReadSchema);
    }

    if (configuration.getBoolean(AVRO_COMPATIBILITY, AVRO_DEFAULT_COMPATIBILITY)) {
      metadata.put(AVRO_COMPATIBILITY, "true");
    }

    return new ReadContext(projection, metadata);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    return prepareForRead(new HadoopParquetConfiguration(configuration), keyValueMetaData, fileSchema, readContext);
  }

  @Override
  public RecordMaterializer<T> prepareForRead(
      ParquetConfiguration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext) {
    Map<String, String> metadata = readContext.getReadSupportMetadata();
    MessageType parquetSchema = readContext.getRequestedSchema();
    Schema avroSchema;

    if (metadata.get(AVRO_READ_SCHEMA_METADATA_KEY) != null) {
      // use the Avro read schema provided by the user
      avroSchema = new Schema.Parser().parse(metadata.get(AVRO_READ_SCHEMA_METADATA_KEY));
    } else if (keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY) != null) {
      // use the Avro schema from the file metadata if present
      avroSchema = new Schema.Parser().parse(keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY));
    } else if (keyValueMetaData.get(OLD_AVRO_SCHEMA_METADATA_KEY) != null) {
      // use the Avro schema from the file metadata if present
      avroSchema = new Schema.Parser().parse(keyValueMetaData.get(OLD_AVRO_SCHEMA_METADATA_KEY));
    } else {
      // default to converting the Parquet schema into an Avro schema
      avroSchema = new AvroSchemaConverter(getDerivedListEncodingConf(configuration, fileSchema))
          .convert(parquetSchema);
    }

    GenericData model = getDataModel(configuration, avroSchema);
    String compatEnabled = metadata.get(AvroReadSupport.AVRO_COMPATIBILITY);
    if (Boolean.parseBoolean(compatEnabled)) {
      return newCompatMaterializer(parquetSchema, avroSchema, model);
    }
    String[] serializableClasses = configuration.getStrings(SERIALIZABLE_CLASSES, null);

    return new AvroRecordMaterializer<T>(
        parquetSchema,
        avroSchema,
        model,
        serializableClasses == null
            ? new ReflectClassValidator.PackageValidator()
            : new ReflectClassValidator.ClassValidator(serializableClasses));
  }

  @SuppressWarnings("unchecked")
  private static <T> RecordMaterializer<T> newCompatMaterializer(
      MessageType parquetSchema, Schema avroSchema, GenericData model) {
    return (RecordMaterializer<T>) new AvroCompatRecordMaterializer(parquetSchema, avroSchema, model);
  }

  private GenericData getDataModel(ParquetConfiguration conf, Schema schema) {
    if (model != null) {
      return model;
    }

    if (conf.get(AVRO_DATA_SUPPLIER) == null && schema != null) {
      GenericData modelForSchema;
      try {
        modelForSchema = AvroRecordConverter.getModelForSchema(schema);
      } catch (Exception e) {
        LOG.warn(
            String.format(
                "Failed to derive data model for Avro schema %s. Parquet will use default "
                    + "SpecificData model for reading from source.",
                schema),
            e);
        modelForSchema = null;
      }

      if (modelForSchema != null) {
        return modelForSchema;
      }
    }

    Class<? extends AvroDataSupplier> suppClass =
        conf.getClass(AVRO_DATA_SUPPLIER, SpecificDataSupplier.class, AvroDataSupplier.class);
    return ReflectionUtils.newInstance(suppClass, ConfigurationUtil.createHadoopConfiguration(conf))
        .get();
  }

  // Returns a ParquetConfiguration with appropriate list-decoding properties set, inferred from
  // the file schema as well as user-supplied Configuration properties.
  // If no configuration changes are required, the original ParquetConfiguration object will be;
  // returned; otherwise, a copy will be created with the correct properties.
  private static ParquetConfiguration getDerivedListEncodingConf(
      ParquetConfiguration configuration, MessageType fileSchema) {
    final boolean autoDetectListStructure =
        configuration.getBoolean(AUTO_DETECT_LIST_STRUCTURE, AUTO_DETECT_LIST_STRUCTURE_DEFAULT);

    if (!autoDetectListStructure
        || configuration.get(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE) != null
        || configuration.get(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS) != null
        || !writesNewListStructure(fileSchema)) {
      return configuration;
    }

    // Avoid mutating the original Configuration by creating a copy, with new properties set
    final ParquetConfiguration copiedConfiguration = new PlainParquetConfiguration();
    for (Map.Entry<String, String> property : configuration) {
      copiedConfiguration.set(property.getKey(), property.getValue());
    }
    copiedConfiguration.setBoolean(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, false);
    copiedConfiguration.setBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, false);

    return copiedConfiguration;
  }

  // The projection converted from the Avro schema writes every list with the same structure, so it
  // only selects a list's columns if the file wrote the list that way too. Use the repeated and
  // element names of each list in the file instead; the converters read all the
  // backward-compatible list structures.
  private static List<Type> alignListStructures(
      GroupType projected, GroupType file, Schema record, Schema readRecord, boolean oldListStructure) {
    List<Type> fields = new ArrayList<>(projected.getFieldCount());
    for (Type field : projected.getFields()) {
      Schema.Field avroField = record.getField(field.getName());
      if (avroField != null && file.containsField(field.getName())) {
        fields.add(alignListStructures(
            field,
            file.getType(field.getName()),
            avroField.schema(),
            readField(readRecord, field.getName()),
            oldListStructure));
      } else {
        fields.add(field);
      }
    }
    return fields;
  }

  private static Type alignListStructures(
      Type projected, Type file, Schema schema, Schema readSchema, boolean oldListStructure) {
    if (projected.isPrimitive() || file.isPrimitive()) {
      return projected;
    }
    GroupType projectedGroup = projected.asGroupType();
    GroupType fileGroup = file.asGroupType();
    Schema nonNullSchema = AvroSchemaConverter.getNonNull(schema);
    switch (nonNullSchema.getType()) {
      case RECORD:
        return projectedGroup.withNewFields(
            alignListStructures(projectedGroup, fileGroup, nonNullSchema, readSchema, oldListStructure));
      case ARRAY:
        return alignList(
            projectedGroup,
            fileGroup,
            nonNullSchema.getElementType(),
            readChild(readSchema, Schema.Type.ARRAY),
            oldListStructure);
      case MAP:
        return alignMap(
            projectedGroup,
            fileGroup,
            nonNullSchema.getValueType(),
            readChild(readSchema, Schema.Type.MAP),
            oldListStructure);
      default:
        return projected;
    }
  }

  private static Type alignList(
      GroupType projected,
      GroupType file,
      Schema elementSchema,
      Schema readElementSchema,
      boolean oldListStructure) {
    if (!(projected.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
        || !(file.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
        || file.isRepetition(Type.Repetition.REPEATED)
        || file.getFieldCount() != 1
        || !file.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      return projected;
    }
    Type projectedRepeated = projected.getType(0);
    Type projectedElement = oldListStructure
        ? projectedRepeated
        : projectedRepeated.asGroupType().getType(0);
    Type fileRepeated = file.getType(0);
    Type alignedRepeated;
    // Decide which field is the element the same way AvroCollectionConverter does
    boolean repeatedIsElement =
        AvroRecordConverter.isElementType(fileRepeated, AvroSchemaConverter.getNonNull(elementSchema));
    if (repeatedIsElement) {
      alignedRepeated =
          alignElement(projectedElement, fileRepeated, elementSchema, readElementSchema, oldListStructure);
    } else {
      GroupType fileRepeatedGroup = fileRepeated.asGroupType();
      Type alignedElement = alignElement(
          projectedElement, fileRepeatedGroup.getType(0), elementSchema, readElementSchema, oldListStructure);
      alignedRepeated = alignedElement == null ? null : fileRepeatedGroup.withNewFields(alignedElement);
    }
    // The converter decides again on the aligned repeated field, with the schema it reads with. Pruning
    // a group element can change that decision, for example when the read schema requires a field the
    // projection leaves out, so keep the converted projection rather than misread the list.
    if (alignedRepeated == null
        || (readElementSchema != null
            && AvroRecordConverter.isElementType(
                    alignedRepeated, AvroSchemaConverter.getNonNull(readElementSchema))
                != repeatedIsElement)) {
      return projected;
    }
    return projected.withNewFields(alignedRepeated);
  }

  private static Type alignMap(
      GroupType projected, GroupType file, Schema valueSchema, Schema readValueSchema, boolean oldListStructure) {
    if (projected.getFieldCount() != 1
        || file.getFieldCount() != 1
        || file.getType(0).isPrimitive()
        || !file.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      return projected;
    }
    GroupType projectedKeyValue = projected.getType(0).asGroupType();
    GroupType fileKeyValue = file.getType(0).asGroupType();
    if (projectedKeyValue.getFieldCount() != 2 || fileKeyValue.getFieldCount() != 2) {
      return projected;
    }
    Type alignedValue = alignElement(
        projectedKeyValue.getType(1), fileKeyValue.getType(1), valueSchema, readValueSchema, oldListStructure);
    return alignedValue == null
        ? projected
        : projected.withNewFields(fileKeyValue.withNewFields(fileKeyValue.getType(0), alignedValue));
  }

  // Returns the projected element under the name of the file's element, or null if one of them is a
  // group and the other is not. The type stays the projected one, so the requested schema is still
  // checked against the file. The repetition is the one the converted projection gives the element:
  // repeated if the file's repeated field is the element, otherwise optional only if the Avro schema
  // allows null.
  private static Type alignElement(
      Type projected, Type file, Schema schema, Schema readSchema, boolean oldListStructure) {
    if (projected.isPrimitive() != file.isPrimitive()) {
      return null;
    }
    Type.Repetition repetition;
    if (file.isRepetition(Type.Repetition.REPEATED)) {
      repetition = Type.Repetition.REPEATED;
    } else if (isNullable(schema)) {
      repetition = Type.Repetition.OPTIONAL;
    } else {
      repetition = Type.Repetition.REQUIRED;
    }
    if (file.isPrimitive()) {
      PrimitiveType primitive = projected.asPrimitiveType();
      Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(
              primitive.getPrimitiveTypeName(), repetition)
          .as(primitive.getLogicalTypeAnnotation());
      if (primitive.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
        builder = builder.length(primitive.getTypeLength());
      }
      return builder.named(file.getName());
    }
    GroupType aligned = alignListStructures(projected, file, schema, readSchema, oldListStructure)
        .asGroupType();
    return Types.buildGroup(repetition)
        .as(aligned.getLogicalTypeAnnotation())
        .addFields(aligned.getFields().toArray(new Type[0]))
        .named(file.getName());
  }

  // The Avro schema the converters read with, if it is not converted from the projection itself, as
  // prepareForRead chooses it.
  private static Schema materializedSchema(ParquetConfiguration configuration, Map<String, String> keyValueMetaData) {
    String schema = configuration.get(AVRO_READ_SCHEMA);
    if (schema == null) {
      schema = keyValueMetaData.get(AVRO_SCHEMA_METADATA_KEY);
    }
    if (schema == null) {
      schema = keyValueMetaData.get(OLD_AVRO_SCHEMA_METADATA_KEY);
    }
    return schema == null ? null : new Schema.Parser().parse(schema);
  }

  private static Schema readField(Schema readRecord, String name) {
    if (readRecord == null) {
      return null;
    }
    Schema record = AvroSchemaConverter.getNonNull(readRecord);
    Schema.Field field = record.getType() == Schema.Type.RECORD ? record.getField(name) : null;
    return field == null ? null : field.schema();
  }

  private static Schema readChild(Schema readSchema, Schema.Type type) {
    if (readSchema == null) {
      return null;
    }
    Schema schema = AvroSchemaConverter.getNonNull(readSchema);
    if (schema.getType() != type) {
      return null;
    }
    return type == Schema.Type.ARRAY ? schema.getElementType() : schema.getValueType();
  }

  private static boolean isNullable(Schema schema) {
    return schema.getType() == Schema.Type.UNION
        && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
  }

  private static boolean writesNewListStructure(MessageType schema) {
    return Boolean.TRUE.equals(allListStructuresAreThreeLevel(schema));
  }

  // Given a Parquet schema, return true only if the schema:
  // - contains one or more List fields
  // - encodes every List field using 3-level list structure
  private static Boolean allListStructuresAreThreeLevel(Type type) {
    if (type.isPrimitive()) {
      return null;
    }
    GroupType group = type.asGroupType();
    if (group.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      if (group.isRepetition(Type.Repetition.REPEATED) || group.getFieldCount() != 1) {
        return false;
      }
      Type repeated = group.getType(0);
      if (repeated.isPrimitive()
          || !repeated.isRepetition(Type.Repetition.REPEATED)
          || !repeated.getName().equals("list")
          || repeated.asGroupType().getFieldCount() != 1) {
        return false;
      }
      Type element = repeated.asGroupType().getType(0);
      if (element.isRepetition(Type.Repetition.REPEATED)
          || !element.getName().equals("element")) {
        return false;
      }
      return !Boolean.FALSE.equals(allListStructuresAreThreeLevel(element));
    }
    Boolean result = null;
    for (Type field : group.getFields()) {
      Boolean fieldListStructuresAreThreeLevel = allListStructuresAreThreeLevel(field);
      if (Boolean.FALSE.equals(fieldListStructuresAreThreeLevel)) {
        return false;
      }
      if (Boolean.TRUE.equals(fieldListStructuresAreThreeLevel)) {
        result = true;
      }
    }
    return result;
  }
}
