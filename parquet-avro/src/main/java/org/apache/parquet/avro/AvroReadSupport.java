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
import java.util.IdentityHashMap;
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
      projection = project(configuration, keyValueMetaData, fileSchema, requestedProjectionString).parquetSchema;
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
    } else if (configuration.get(AVRO_REQUESTED_PROJECTION) != null) {
      // the schema converted from the requested projection, with the record names init aligned
      avroSchema = project(
              configuration, keyValueMetaData, fileSchema, configuration.get(AVRO_REQUESTED_PROJECTION))
          .avroSchema;
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

  // The requested projection, with the list structures of the file, and the Avro schema the converters
  // read it with
  private static final class Projection {
    private final MessageType parquetSchema;
    private final Schema avroSchema;

    private Projection(MessageType parquetSchema, Schema avroSchema) {
      this.parquetSchema = parquetSchema;
      this.avroSchema = avroSchema;
    }
  }

  private static final class Alignment {
    private final boolean oldListStructure;
    // Whether the converters read with the schema converted from the projection, as they do without a
    // read schema or an Avro schema in the file. Its record names come from the Parquet names of the
    // projection, not from the caller, so the alignment may give a list's element record the file's name
    // of the element, as a read without a projection names it.
    private final boolean convertedReadSchema;
    private final Map<Schema, String> recordNames = new IdentityHashMap<>();

    private Alignment(boolean oldListStructure, boolean convertedReadSchema) {
      this.oldListStructure = oldListStructure;
      this.convertedReadSchema = convertedReadSchema;
    }
  }

  // init and prepareForRead both call this, so prepareForRead gets the converted schema without its
  // JSON: two records of a converted schema can have the same full name, which the JSON can't hold.
  private static Projection project(
      ParquetConfiguration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      String requestedProjection) {
    Schema avroRequestedProjection = new Schema.Parser().parse(requestedProjection);
    ParquetConfiguration listEncodingConf = getDerivedListEncodingConf(configuration, fileSchema);
    MessageType projection = new AvroSchemaConverter(listEncodingConf).convert(avroRequestedProjection);
    Schema readSchema = materializedSchema(configuration, keyValueMetaData);
    boolean convertedReadSchema = readSchema == null;
    if (convertedReadSchema) {
      // As before: converting the aligned projection instead could give its lists another shape, such as
      // records for the wrappers of their elements
      readSchema = new AvroSchemaConverter(listEncodingConf).convert(projection);
    }
    Alignment alignment = new Alignment(
        listEncodingConf.getBoolean(
            AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT),
        convertedReadSchema);
    MessageType aligned = new MessageType(
        projection.getName(),
        alignListStructures(projection, fileSchema, avroRequestedProjection, readSchema, alignment));
    return new Projection(
        aligned, convertedReadSchema ? withRecordNames(readSchema, alignment.recordNames) : readSchema);
  }

  // The projection converted from the Avro schema writes every list with the same structure, so it
  // only selects a list's columns if the file wrote the list that way too. Use the repeated and
  // element names of each list in the file instead; the converters read all the
  // backward-compatible list structures.
  private static List<Type> alignListStructures(
      GroupType projected, GroupType file, Schema record, Schema readRecord, Alignment alignment) {
    List<Type> fields = new ArrayList<>(projected.getFieldCount());
    for (Type field : projected.getFields()) {
      Schema.Field avroField = record.getField(field.getName());
      // A LIST field of a record must not be repeated; only the element of a two-level list can be
      if (avroField != null
          && file.containsField(field.getName())
          && !isRepeatedList(file.getType(field.getName()))) {
        fields.add(alignListStructures(
            field,
            file.getType(field.getName()),
            avroField.schema(),
            readField(readRecord, field.getName()),
            alignment));
      } else {
        fields.add(field);
      }
    }
    return fields;
  }

  private static Type alignListStructures(
      Type projected, Type file, Schema schema, Schema readSchema, Alignment alignment) {
    if (projected.isPrimitive() || file.isPrimitive()) {
      return projected;
    }
    GroupType projectedGroup = projected.asGroupType();
    GroupType fileGroup = file.asGroupType();
    Schema nonNullSchema = AvroSchemaConverter.getNonNull(schema);
    switch (nonNullSchema.getType()) {
      case RECORD:
        return projectedGroup.withNewFields(
            alignListStructures(projectedGroup, fileGroup, nonNullSchema, readSchema, alignment));
      case ARRAY:
        return alignList(
            projectedGroup,
            fileGroup,
            nonNullSchema.getElementType(),
            readChild(readSchema, Schema.Type.ARRAY),
            alignment);
      case MAP:
        return alignMap(
            projectedGroup,
            fileGroup,
            nonNullSchema.getValueType(),
            readChild(readSchema, Schema.Type.MAP),
            alignment);
      default:
        return projected;
    }
  }

  private static Type alignList(
      GroupType projected, GroupType file, Schema elementSchema, Schema readElementSchema, Alignment alignment) {
    // The list itself may be repeated, as the element of a two-level list
    if (!(projected.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
        || !(file.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
        || file.getFieldCount() != 1
        || !file.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      return projected;
    }
    Type projectedRepeated = projected.getType(0);
    Type projectedElement = alignment.oldListStructure
        ? projectedRepeated
        : projectedRepeated.asGroupType().getType(0);
    Type fileRepeated = file.getType(0);
    Schema readElement = readElementSchema == null ? null : AvroSchemaConverter.getNonNull(readElementSchema);
    // Decide which field is the element the same way AvroCollectionConverter does, with the schema it
    // reads with. Its structural rules don't depend on the projection, so check them first, and align
    // the repeated field as a wrapper only if they don't decide.
    Type alignedRepeated;
    boolean repeatedIsElement;
    // The read element under the name the alignment gives it, which the converter checks
    Schema readElementRecord = readElement;
    if (AvroRecordConverter.isElementType(fileRepeated, null)) {
      repeatedIsElement = true;
      alignedRepeated = alignElement(projectedElement, fileRepeated, elementSchema, readElementSchema, alignment);
    } else {
      // A group with one field that isn't repeated: the group is the element if the element is a record
      // that it matches, such as points_tuple { x } for a list of records with x, and otherwise it
      // wraps the element. The checks convert the repeated field to Avro, so give them only the
      // projected columns: a column the projection doesn't select, such as a MAP with integer keys, can
      // have a type that Avro can not represent.
      GroupType fileRepeatedGroup = fileRepeated.asGroupType();
      Type direct = null;
      Schema directRecord = readElement;
      if (readElement != null
          && readElement.getType() == Schema.Type.RECORD
          && !projectedElement.isPrimitive()
          && hasFields(fileRepeatedGroup, projectedElement.asGroupType())) {
        direct = alignElement(projectedElement, fileRepeated, elementSchema, readElementSchema, alignment);
        if (direct != null && alignment.convertedReadSchema) {
          Map<Schema, String> names = new IdentityHashMap<>(alignment.recordNames);
          names.put(readElement, fileRepeated.getName());
          directRecord = withRecordNames(readElement, names);
        }
      }
      if (direct != null
          && compatible(direct, fileRepeated)
          && AvroRecordConverter.isElementType(direct, directRecord)) {
        repeatedIsElement = true;
        alignedRepeated = direct;
        readElementRecord = directRecord;
      } else {
        repeatedIsElement = false;
        Type wrappedElement = alignElement(
            projectedElement, fileRepeatedGroup.getType(0), elementSchema, readElementSchema, alignment);
        alignedRepeated = wrappedElement == null ? null : fileRepeatedGroup.withNewFields(wrappedElement);
      }
    }
    // Keep the converted projection, as before, unless the converters read the aligned list: each of its
    // columns has the file's type, the read schema can read its element, and the converter decides
    // again, on the aligned repeated field, that the same field is the element. Pruning a group element
    // can change that decision, for example when the read schema requires a field the projection leaves
    // out.
    if (alignedRepeated == null
        || !compatible(alignedRepeated, fileRepeated)
        || !readsAs(
            repeatedIsElement
                ? alignedRepeated
                : alignedRepeated.asGroupType().getType(0),
            readElement)
        || repeatedIsElement != AvroRecordConverter.isElementType(alignedRepeated, readElementRecord)) {
      return projected;
    }
    if (readElementRecord != readElement) {
      alignment.recordNames.put(readElement, fileRepeated.getName());
    }
    return projected.withNewFields(alignedRepeated);
  }

  private static Type alignMap(
      GroupType projected, GroupType file, Schema valueSchema, Schema readValueSchema, Alignment alignment) {
    // Only a MAP in the file: a list of key-value records would lose the entries that repeat a key
    if (!isMap(file)) {
      return projected;
    }
    if (projected.getFieldCount() != 1
        || file.getFieldCount() != 1
        || file.getType(0).isPrimitive()
        || !file.getType(0).isRepetition(Type.Repetition.REPEATED)) {
      return projected;
    }
    GroupType projectedKeyValue = projected.getType(0).asGroupType();
    GroupType fileKeyValue = file.getType(0).asGroupType();
    if (projectedKeyValue.getFieldCount() != 2
        || fileKeyValue.getFieldCount() != 2
        || !isStringKey(fileKeyValue.getType(0))) {
      return projected;
    }
    // Keep the projected key type, so the requested string key is still checked against the file
    Type alignedKey = alignElement(
        projectedKeyValue.getType(0),
        fileKeyValue.getType(0),
        Schema.create(Schema.Type.STRING),
        null,
        alignment);
    Type alignedValue = alignElement(
        projectedKeyValue.getType(1), fileKeyValue.getType(1), valueSchema, readValueSchema, alignment);
    return alignedKey == null || alignedValue == null
        ? projected
        : projected.withNewFields(fileKeyValue.withNewFields(alignedKey, alignedValue));
  }

  // Returns the projected element under the name of the file's element, or null if one of them is a
  // group and the other is not. The type stays the projected one, so the requested schema is still
  // checked against the file. The repetition is the one the converted projection gives the element:
  // repeated if the file's repeated field is the element, otherwise optional only if the Avro schema
  // allows null.
  private static Type alignElement(Type projected, Type file, Schema schema, Schema readSchema, Alignment alignment) {
    if (projected.isPrimitive() != file.isPrimitive()
        || (!projected.isPrimitive() && !sameKind(projected.asGroupType(), file.asGroupType()))) {
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
    GroupType aligned = alignListStructures(projected, file, schema, readSchema, alignment)
        .asGroupType();
    return Types.buildGroup(repetition)
        .as(aligned.getLogicalTypeAnnotation())
        .addFields(aligned.getFields().toArray(new Type[0]))
        .named(file.getName());
  }

  // Whether each field of the requested type that the file has is a group or a primitive in both, as
  // ColumnIO requires. A field the file doesn't have reads as null.
  private static boolean compatible(Type requested, Type file) {
    if (requested.isPrimitive() || file.isPrimitive()) {
      return requested.isPrimitive() == file.isPrimitive();
    }
    GroupType fileGroup = file.asGroupType();
    return requested.asGroupType().getFields().stream()
        .allMatch(field -> !fileGroup.containsField(field.getName())
            || compatible(field, fileGroup.getType(field.getName())));
  }

  // Whether the converters can read the element with the read schema's element: a record from a group
  // that is neither a LIST nor a MAP, a map from a MAP, an array from a group or a repeated field, and
  // any other type from a primitive
  private static boolean readsAs(Type element, Schema readElement) {
    if (readElement == null) {
      return true;
    }
    switch (readElement.getType()) {
      case RECORD:
        return !element.isPrimitive()
            && !(element.getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation)
            && !isMap(element.asGroupType());
      case MAP:
        return !element.isPrimitive() && isMap(element.asGroupType());
      case ARRAY:
        return !element.isPrimitive() || element.isRepetition(Type.Repetition.REPEATED);
      case UNION:
        return true;
      default:
        return element.isPrimitive();
    }
  }

  // Avro reads a map's keys as strings. BINARY keys the file doesn't annotate as strings may not be UTF-8,
  // and two of them could decode to the same string, which would overwrite an entry. Keys of another
  // type still fail the check of the requested string key.
  private static boolean isStringKey(Type key) {
    if (!key.isPrimitive()
        || key.asPrimitiveType().getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY) {
      return true;
    }
    LogicalTypeAnnotation annotation = key.getLogicalTypeAnnotation();
    return annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation
        || annotation instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
  }

  // A copy of the schema whose records have the given names. The schema is converted from Parquet, so it
  // has no recursive records.
  private static Schema withRecordNames(Schema schema, Map<Schema, String> names) {
    if (names.isEmpty()) {
      return schema;
    }
    switch (schema.getType()) {
      case RECORD: {
        List<Schema.Field> fields = new ArrayList<>(schema.getFields().size());
        boolean changed = names.containsKey(schema);
        for (Schema.Field field : schema.getFields()) {
          Schema fieldSchema = withRecordNames(field.schema(), names);
          changed |= fieldSchema != field.schema();
          fields.add(new Schema.Field(field, fieldSchema));
        }
        if (!changed) {
          return schema;
        }
        Schema record = Schema.createRecord(
            names.getOrDefault(schema, schema.getName()),
            schema.getDoc(),
            schema.getNamespace(),
            schema.isError(),
            fields);
        schema.getObjectProps().forEach(record::addProp);
        return record;
      }
      case ARRAY: {
        Schema element = withRecordNames(schema.getElementType(), names);
        return element == schema.getElementType() ? schema : Schema.createArray(element);
      }
      case MAP: {
        Schema value = withRecordNames(schema.getValueType(), names);
        return value == schema.getValueType() ? schema : Schema.createMap(value);
      }
      case UNION: {
        List<Schema> types = new ArrayList<>(schema.getTypes().size());
        boolean changed = false;
        for (Schema type : schema.getTypes()) {
          Schema renamed = withRecordNames(type, names);
          changed |= renamed != type;
          types.add(renamed);
        }
        return changed ? Schema.createUnion(types) : schema;
      }
      default:
        return schema;
    }
  }

  // Whether the group has a field for each field of the projected group
  private static boolean hasFields(GroupType group, GroupType projected) {
    return projected.getFields().stream().allMatch(field -> group.containsField(field.getName()));
  }

  // Whether the groups are both LISTs, both MAPs, or neither
  private static boolean sameKind(GroupType projected, GroupType file) {
    boolean projectedList =
        projected.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    boolean fileList = file.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    return projectedList == fileList && isMap(projected) == isMap(file);
  }

  private static boolean isRepeatedList(Type type) {
    return type.isRepetition(Type.Repetition.REPEATED)
        && type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
  }

  // Whether the group is a MAP in the file, also with the MAP_KEY_VALUE annotation older writers use
  private static boolean isMap(GroupType group) {
    LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
    return annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
        || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation
        || (group.getFieldCount() == 1
            && group.getType(0).getLogicalTypeAnnotation()
                instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation);
  }

  // The Avro schema the converters read with, as prepareForRead chooses it, or null if the file has
  // none and the caller sets no read schema.
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
    if (record.getType() != Schema.Type.RECORD) {
      return null;
    }
    Schema.Field field = record.getField(name);
    if (field == null) {
      // The converters also match a field of the read schema by its aliases
      field = record.getFields().stream()
          .filter(f -> f.aliases().contains(name))
          .findFirst()
          .orElse(null);
    }
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
