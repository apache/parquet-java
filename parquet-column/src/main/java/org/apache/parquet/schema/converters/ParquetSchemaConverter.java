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
package org.apache.parquet.schema.converters;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.apache.parquet.schema.converters.ParquetEnumConverter.fromParquetEdgeInterpolationAlgorithm;
import static org.apache.parquet.schema.converters.ParquetEnumConverter.getPrimitive;
import static org.apache.parquet.schema.converters.ParquetEnumConverter.getType;
import static org.apache.parquet.schema.converters.ParquetEnumConverter.toParquetRepetition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.IntType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.LogicalTypes;
import org.apache.parquet.format.MicroSeconds;
import org.apache.parquet.format.MilliSeconds;
import org.apache.parquet.format.NanoSeconds;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.TimeType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.TimestampType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.format.VariantType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.TypeVisitor;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class converts between the thrift based {@link org.apache.parquet.format.SchemaElement} class and the
 * parquet-column {@link org.apache.parquet.column.ColumnDescriptor} classes.
 */
public class ParquetSchemaConverter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetSchemaConverter.class);
  private static final TypeDefinedOrder TYPE_DEFINED_ORDER = new TypeDefinedOrder();
  private static final LogicalTypeConverterVisitor LOGICAL_TYPE_ANNOTATION_VISITOR =
      new LogicalTypeConverterVisitor();
  private static final ConvertedTypeConverterVisitor CONVERTED_TYPE_CONVERTER_VISITOR =
      new ConvertedTypeConverterVisitor();

  public ParquetSchemaConverter() {}

  /**
   * Create parquet-column MessageType from {@link SchemaElement} and {@link ColumnOrder} thrift objects
   *
   * @param schema the {@link MessageType} schema
   * @return the ordering defined for each of the columns
   */
  public MessageType fromParquetSchema(List<SchemaElement> schema, List<ColumnOrder> columnOrders) {
    Iterator<SchemaElement> iterator = schema.iterator();
    SchemaElement root = iterator.next();
    Types.MessageTypeBuilder builder = Types.buildMessage();
    if (root.isSetField_id()) {
      builder.id(root.field_id);
    }
    buildChildren(builder, iterator, root.getNum_children(), columnOrders, 0);
    return builder.named(root.name);
  }

  private void buildChildren(
      Types.GroupBuilder builder,
      Iterator<SchemaElement> schema,
      int childrenCount,
      List<ColumnOrder> columnOrders,
      int columnCount) {
    for (int i = 0; i < childrenCount; i++) {
      SchemaElement schemaElement = schema.next();

      // Create Parquet Type.
      Types.Builder childBuilder;
      if (schemaElement.type != null) {
        Types.PrimitiveBuilder primitiveBuilder = builder.primitive(
            getPrimitive(schemaElement.type),
            ParquetEnumConverter.fromParquetRepetition(schemaElement.repetition_type));
        if (schemaElement.isSetType_length()) {
          primitiveBuilder.length(schemaElement.type_length);
        }
        if (schemaElement.isSetPrecision()) {
          primitiveBuilder.precision(schemaElement.precision);
        }
        if (schemaElement.isSetScale()) {
          primitiveBuilder.scale(schemaElement.scale);
        }
        if (columnOrders != null) {
          org.apache.parquet.schema.ColumnOrder columnOrder =
              fromParquetColumnOrder(columnOrders.get(columnCount));
          // As per parquet format 2.4.0 no UNDEFINED order is supported. So, set undefined column order for
          // the types
          // where ordering is not supported.
          if (columnOrder.getColumnOrderName()
                  == org.apache.parquet.schema.ColumnOrder.ColumnOrderName.TYPE_DEFINED_ORDER
              && (schemaElement.type == Type.INT96
                  || schemaElement.converted_type == ConvertedType.INTERVAL)) {
            columnOrder = org.apache.parquet.schema.ColumnOrder.undefined();
          }
          primitiveBuilder.columnOrder(columnOrder);
        }
        childBuilder = primitiveBuilder;
      } else {
        childBuilder = builder.group(ParquetEnumConverter.fromParquetRepetition(schemaElement.repetition_type));
        buildChildren(
            (Types.GroupBuilder) childBuilder,
            schema,
            schemaElement.num_children,
            columnOrders,
            columnCount);
      }

      if (schemaElement.isSetLogicalType()) {
        childBuilder.as(getLogicalTypeAnnotation(schemaElement.logicalType));
      }
      if (schemaElement.isSetConverted_type()) {
        OriginalType originalType = getLogicalTypeAnnotation(schemaElement.converted_type, schemaElement)
            .toOriginalType();
        OriginalType newOriginalType = (schemaElement.isSetLogicalType()
                && getLogicalTypeAnnotation(schemaElement.logicalType) != null)
            ? getLogicalTypeAnnotation(schemaElement.logicalType).toOriginalType()
            : null;
        if (!originalType.equals(newOriginalType)) {
          if (newOriginalType != null) {
            LOG.warn(
                "Converted type and logical type metadata mismatch (convertedType: {}, logical type: {}). Using value in converted type.",
                schemaElement.converted_type,
                schemaElement.logicalType);
          }
          childBuilder.as(originalType);
        }
      }
      if (schemaElement.isSetField_id()) {
        childBuilder.id(schemaElement.field_id);
      }

      childBuilder.named(schemaElement.name);
      ++columnCount;
    }
  }

  private static org.apache.parquet.schema.ColumnOrder fromParquetColumnOrder(ColumnOrder columnOrder) {
    if (columnOrder.isSetTYPE_ORDER()) {
      return org.apache.parquet.schema.ColumnOrder.typeDefined();
    }
    // The column order is not yet supported by this API
    return org.apache.parquet.schema.ColumnOrder.undefined();
  }

  // Visible for testing
  LogicalTypeAnnotation getLogicalTypeAnnotation(ConvertedType type, SchemaElement schemaElement) {
    switch (type) {
      case UTF8:
        return LogicalTypeAnnotation.stringType();
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case MAP_KEY_VALUE:
        return LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case DECIMAL:
        int scale = (schemaElement == null ? 0 : schemaElement.scale);
        int precision = (schemaElement == null ? 0 : schemaElement.precision);
        return LogicalTypeAnnotation.decimalType(scale, precision);
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case TIME_MILLIS:
        return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIME_MICROS:
        return LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case TIMESTAMP_MILLIS:
        return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS);
      case TIMESTAMP_MICROS:
        return LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS);
      case INTERVAL:
        return LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance();
      case INT_8:
        return LogicalTypeAnnotation.intType(8, true);
      case INT_16:
        return LogicalTypeAnnotation.intType(16, true);
      case INT_32:
        return LogicalTypeAnnotation.intType(32, true);
      case INT_64:
        return LogicalTypeAnnotation.intType(64, true);
      case UINT_8:
        return LogicalTypeAnnotation.intType(8, false);
      case UINT_16:
        return LogicalTypeAnnotation.intType(16, false);
      case UINT_32:
        return LogicalTypeAnnotation.intType(32, false);
      case UINT_64:
        return LogicalTypeAnnotation.intType(64, false);
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      default:
        throw new RuntimeException(
            "Can't convert converted type to logical type, unknown converted type " + type);
    }
  }

  LogicalTypeAnnotation getLogicalTypeAnnotation(LogicalType type) {
    switch (type.getSetField()) {
      case MAP:
        return LogicalTypeAnnotation.mapType();
      case BSON:
        return LogicalTypeAnnotation.bsonType();
      case DATE:
        return LogicalTypeAnnotation.dateType();
      case ENUM:
        return LogicalTypeAnnotation.enumType();
      case JSON:
        return LogicalTypeAnnotation.jsonType();
      case LIST:
        return LogicalTypeAnnotation.listType();
      case TIME:
        TimeType time = type.getTIME();
        return LogicalTypeAnnotation.timeType(
            time.isAdjustedToUTC, ParquetEnumConverter.convertTimeUnit(time.unit));
      case STRING:
        return LogicalTypeAnnotation.stringType();
      case DECIMAL:
        DecimalType decimal = type.getDECIMAL();
        return LogicalTypeAnnotation.decimalType(decimal.scale, decimal.precision);
      case INTEGER:
        IntType integer = type.getINTEGER();
        return LogicalTypeAnnotation.intType(integer.bitWidth, integer.isSigned);
      case UNKNOWN:
        return LogicalTypeAnnotation.unknownType();
      case TIMESTAMP:
        TimestampType timestamp = type.getTIMESTAMP();
        return LogicalTypeAnnotation.timestampType(
            timestamp.isAdjustedToUTC, ParquetEnumConverter.convertTimeUnit(timestamp.unit));
      case UUID:
        return LogicalTypeAnnotation.uuidType();
      case FLOAT16:
        return LogicalTypeAnnotation.float16Type();
      case GEOMETRY:
        GeometryType geometry = type.getGEOMETRY();
        return LogicalTypeAnnotation.geometryType(geometry.getCrs());
      case GEOGRAPHY:
        GeographyType geography = type.getGEOGRAPHY();
        return LogicalTypeAnnotation.geographyType(
            geography.getCrs(),
            ParquetEnumConverter.toParquetEdgeInterpolationAlgorithm(geography.getAlgorithm()));
      case VARIANT:
        VariantType variant = type.getVARIANT();
        return LogicalTypeAnnotation.variantType(variant.getSpecification_version());
      default:
        throw new RuntimeException("Unknown logical type " + type);
    }
  }

  /**
   * Parse parquet-column MessageType and write the {@link ColumnOrder} objects for it.
   *
   * @param schema the {@link MessageType} schema
   * @return the ordering defined for each of the columns
   */
  public List<ColumnOrder> getColumnOrders(MessageType schema) {
    List<ColumnOrder> columnOrders = new ArrayList<>();
    // Currently, only TypeDefinedOrder is supported, so we create a column order for each columns with
    // TypeDefinedOrder even if some types (e.g. INT96) have undefined column orders.
    for (int i = 0, n = schema.getPaths().size(); i < n; ++i) {
      ColumnOrder columnOrder = new ColumnOrder();
      columnOrder.setTYPE_ORDER(TYPE_DEFINED_ORDER);
      columnOrders.add(columnOrder);
    }
    return columnOrders;
  }

  /**
   * Parse parquet-column MessageType and write the {@link SchemaElement} objects for it.
   *
   * @param schema the {@link MessageType} schema
   * @return the {@link SchemaElement} objects for this parquet-column schema
   */
  public List<SchemaElement> toParquetSchema(MessageType schema) {
    List<SchemaElement> result = new ArrayList<SchemaElement>();
    addToList(result, schema);
    return result;
  }

  private void addToList(final List<SchemaElement> result, org.apache.parquet.schema.Type field) {
    field.accept(new TypeVisitor() {
      @Override
      public void visit(PrimitiveType primitiveType) {
        SchemaElement element = new SchemaElement(primitiveType.getName());
        element.setRepetition_type(toParquetRepetition(primitiveType.getRepetition()));
        element.setType(getType(primitiveType.getPrimitiveTypeName()));
        if (primitiveType.getLogicalTypeAnnotation() != null) {
          element.setConverted_type(convertToConvertedType(primitiveType.getLogicalTypeAnnotation()));
          element.setLogicalType(convertToLogicalType(primitiveType.getLogicalTypeAnnotation()));
        }
        if (primitiveType.getDecimalMetadata() != null) {
          element.setPrecision(primitiveType.getDecimalMetadata().getPrecision());
          element.setScale(primitiveType.getDecimalMetadata().getScale());
        }
        if (primitiveType.getTypeLength() > 0) {
          element.setType_length(primitiveType.getTypeLength());
        }
        if (primitiveType.getId() != null) {
          element.setField_id(primitiveType.getId().intValue());
        }
        result.add(element);
      }

      @Override
      public void visit(MessageType messageType) {
        SchemaElement element = new SchemaElement(messageType.getName());
        if (messageType.getId() != null) {
          element.setField_id(messageType.getId().intValue());
        }
        visitChildren(result, messageType.asGroupType(), element);
      }

      @Override
      public void visit(GroupType groupType) {
        SchemaElement element = new SchemaElement(groupType.getName());
        element.setRepetition_type(toParquetRepetition(groupType.getRepetition()));
        if (groupType.getLogicalTypeAnnotation() != null) {
          element.setConverted_type(convertToConvertedType(groupType.getLogicalTypeAnnotation()));
          element.setLogicalType(convertToLogicalType(groupType.getLogicalTypeAnnotation()));
        }
        if (groupType.getId() != null) {
          element.setField_id(groupType.getId().intValue());
        }
        visitChildren(result, groupType, element);
      }

      private void visitChildren(final List<SchemaElement> result, GroupType groupType, SchemaElement element) {
        element.setNum_children(groupType.getFieldCount());
        result.add(element);
        for (org.apache.parquet.schema.Type field : groupType.getFields()) {
          addToList(result, field);
        }
      }
    });
  }

  LogicalType convertToLogicalType(LogicalTypeAnnotation logicalTypeAnnotation) {
    return logicalTypeAnnotation.accept(LOGICAL_TYPE_ANNOTATION_VISITOR).orElse(null);
  }

  ConvertedType convertToConvertedType(LogicalTypeAnnotation logicalTypeAnnotation) {
    return logicalTypeAnnotation.accept(CONVERTED_TYPE_CONVERTER_VISITOR).orElse(null);
  }

  static org.apache.parquet.format.TimeUnit convertUnit(LogicalTypeAnnotation.TimeUnit unit) {
    switch (unit) {
      case MICROS:
        return org.apache.parquet.format.TimeUnit.MICROS(new MicroSeconds());
      case MILLIS:
        return org.apache.parquet.format.TimeUnit.MILLIS(new MilliSeconds());
      case NANOS:
        return TimeUnit.NANOS(new NanoSeconds());
      default:
        throw new RuntimeException("Unknown time unit " + unit);
    }
  }

  private static class ConvertedTypeConverterVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ConvertedType> {
    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return of(ConvertedType.UTF8);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
      return of(ConvertedType.MAP);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
      return of(ConvertedType.LIST);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return of(ConvertedType.ENUM);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      return of(ConvertedType.DECIMAL);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return of(ConvertedType.DATE);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      switch (timeLogicalType.getUnit()) {
        case MILLIS:
          return of(ConvertedType.TIME_MILLIS);
        case MICROS:
          return of(ConvertedType.TIME_MICROS);
        case NANOS:
          return empty();
        default:
          throw new RuntimeException("Unknown converted type for " + timeLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(
        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      switch (timestampLogicalType.getUnit()) {
        case MICROS:
          return of(ConvertedType.TIMESTAMP_MICROS);
        case MILLIS:
          return of(ConvertedType.TIMESTAMP_MILLIS);
        case NANOS:
          return empty();
        default:
          throw new RuntimeException("Unknown converted type for " + timestampLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      boolean signed = intLogicalType.isSigned();
      switch (intLogicalType.getBitWidth()) {
        case 8:
          return of(signed ? ConvertedType.INT_8 : ConvertedType.UINT_8);
        case 16:
          return of(signed ? ConvertedType.INT_16 : ConvertedType.UINT_16);
        case 32:
          return of(signed ? ConvertedType.INT_32 : ConvertedType.UINT_32);
        case 64:
          return of(signed ? ConvertedType.INT_64 : ConvertedType.UINT_64);
        default:
          throw new RuntimeException("Unknown original type " + intLogicalType.toOriginalType());
      }
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return of(ConvertedType.JSON);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return of(ConvertedType.BSON);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
      return of(ConvertedType.INTERVAL);
    }

    @Override
    public Optional<ConvertedType> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
      return of(ConvertedType.MAP_KEY_VALUE);
    }
  }

  private static class LogicalTypeConverterVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<LogicalType> {
    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return of(LogicalTypes.UTF8);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
      return of(LogicalTypes.MAP);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
      return of(LogicalTypes.LIST);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return of(LogicalTypes.ENUM);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      return of(LogicalTypes.DECIMAL(decimalLogicalType.getScale(), decimalLogicalType.getPrecision()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return of(LogicalTypes.DATE);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      return of(LogicalType.TIME(
          new TimeType(timeLogicalType.isAdjustedToUTC(), convertUnit(timeLogicalType.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      return of(LogicalType.TIMESTAMP(new TimestampType(
          timestampLogicalType.isAdjustedToUTC(), convertUnit(timestampLogicalType.getUnit()))));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      return of(LogicalType.INTEGER(new IntType((byte) intLogicalType.getBitWidth(), intLogicalType.isSigned())));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return of(LogicalTypes.JSON);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return of(LogicalTypes.BSON);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
      return of(LogicalTypes.UUID);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.Float16LogicalTypeAnnotation float16LogicalType) {
      return of(LogicalTypes.FLOAT16);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.UnknownLogicalTypeAnnotation unknownLogicalType) {
      return of(LogicalTypes.UNKNOWN);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
      return of(LogicalTypes.UNKNOWN);
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.VariantLogicalTypeAnnotation variantLogicalType) {
      return of(LogicalTypes.VARIANT(variantLogicalType.getSpecVersion()));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation geometryLogicalType) {
      GeometryType geometryType = new GeometryType();
      if (geometryLogicalType.getCrs() != null
          && !geometryLogicalType.getCrs().isEmpty()) {
        geometryType.setCrs(geometryLogicalType.getCrs());
      }
      return of(LogicalType.GEOMETRY(geometryType));
    }

    @Override
    public Optional<LogicalType> visit(LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyLogicalType) {
      GeographyType geographyType = new GeographyType();
      if (geographyLogicalType.getCrs() != null
          && !geographyLogicalType.getCrs().isEmpty()) {
        geographyType.setCrs(geographyLogicalType.getCrs());
      }
      geographyType.setAlgorithm(fromParquetEdgeInterpolationAlgorithm(geographyLogicalType.getAlgorithm()));
      return of(LogicalType.GEOGRAPHY(geographyType));
    }
  }
}
