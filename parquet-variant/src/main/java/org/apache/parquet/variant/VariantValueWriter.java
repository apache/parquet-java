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
package org.apache.parquet.variant;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Class to write Variant values to a shredded schema.
 */
public class VariantValueWriter {
  private static final String LIST_REPEATED_NAME = "list";
  private static final String LIST_ELEMENT_NAME = "element";

  private final ByteBuffer metadataBuffer;
  private final RecordConsumer recordConsumer;

  // We defer initializing the ImmutableMetata until it's needed. There is a construction cost to deserialize the
  // metadata binary into a Map, and if all object fields are shredded into typed_value, it will never be used.
  private ImmutableMetadata metadata = null;

  VariantValueWriter(RecordConsumer recordConsumer, ByteBuffer metadata) {
    this.recordConsumer = recordConsumer;
    this.metadataBuffer = metadata;
  }

  Metadata getMetadata() {
    if (metadata == null) {
      metadata = new ImmutableMetadata(metadataBuffer);
    }
    return metadata;
  }

  /**
   * Write a Variant value to a shredded schema.
   */
  public static void write(RecordConsumer recordConsumer, GroupType schema, Variant value) {
    recordConsumer.startGroup();
    int metadataIndex = schema.getFieldIndex("metadata");
    recordConsumer.startField("metadata", metadataIndex);
    recordConsumer.addBinary(Binary.fromConstantByteBuffer(value.getMetadataBuffer()));
    recordConsumer.endField("metadata", metadataIndex);
    VariantValueWriter writer = new VariantValueWriter(recordConsumer, value.getMetadataBuffer());
    writer.write(schema, value);
    recordConsumer.endGroup();
  }

  /**
   * Write a Variant value to a shredded schema. The caller is responsible for calling startGroup()
   * and endGroup(), and writing metadata if this is the top level of the Variant group.
   */
  void write(GroupType schema, Variant value) {
    Type typedValueField = null;
    if (schema.containsField("typed_value")) {
      typedValueField = schema.getType("typed_value");
    }

    Variant.Type variantType = value.getType();

    // Handle typed_value if present
    if (isTypeCompatible(variantType, typedValueField, value)) {
      int typedValueIdx = schema.getFieldIndex("typed_value");
      recordConsumer.startField("typed_value", typedValueIdx);
      ByteBuffer residual = null;
      if (typedValueField.isPrimitive()) {
        writeScalarValue(value);
      } else if (typedValueField.getLogicalTypeAnnotation()
          instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
        writeArrayValue(value, typedValueField.asGroupType());
      } else {
        residual = writeObjectValue(value, typedValueField.asGroupType());
      }
      recordConsumer.endField("typed_value", typedValueIdx);

      if (residual != null) {
        int valueIdx = schema.getFieldIndex("value");
        recordConsumer.startField("value", valueIdx);
        recordConsumer.addBinary(Binary.fromConstantByteBuffer(residual));
        recordConsumer.endField("value", valueIdx);
      }
    } else {
      int valueIdx = schema.getFieldIndex("value");
      recordConsumer.startField("value", valueIdx);
      recordConsumer.addBinary(Binary.fromReusedByteBuffer(value.getValueBuffer()));
      recordConsumer.endField("value", valueIdx);
    }
  }

  // Return true if the logical type is a decimal with the same scale as the provided value, with enough
  // precision to hold the value. The provided value must be a decimal.
  private static boolean compatibleDecimalType(Variant value, LogicalTypeAnnotation logicalType) {
    if (!(logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)) {
      return false;
    }
    LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
        (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType;

    BigDecimal decimal = value.getDecimal();
    return decimal.scale() == decimalType.getScale() && decimal.precision() <= decimalType.getPrecision();
  }

  private static boolean isTypeCompatible(Variant.Type variantType, Type typedValueField, Variant value) {
    if (typedValueField == null) {
      return false;
    }
    if (typedValueField.isPrimitive()) {
      PrimitiveType primitiveType = typedValueField.asPrimitiveType();
      LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
      PrimitiveType.PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();

      switch (variantType) {
        case BOOLEAN:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.BOOLEAN;
        case BYTE:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
              && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).isSigned()
              && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).getBitWidth() == 8;
        case SHORT:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
              && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).isSigned()
              && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).getBitWidth() == 16;
        case INT:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && (logicalType == null
                  || (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
                      && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).isSigned()
                      && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType)
                              .getBitWidth()
                          == 32));
        case LONG:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
              && (logicalType == null
                  || (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
                      && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).isSigned()
                      && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType)
                              .getBitWidth()
                          == 64));
        case FLOAT:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT;
        case DOUBLE:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE;
        case DECIMAL4:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && compatibleDecimalType(value, logicalType);
        case DECIMAL8:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
              && compatibleDecimalType(value, logicalType);
        case DECIMAL16:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
              && compatibleDecimalType(value, logicalType);
        case DATE:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation;
        case TIME:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
              && logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
        case TIMESTAMP_NTZ:
        case TIMESTAMP_NANOS_NTZ:
        case TIMESTAMP_TZ:
        case TIMESTAMP_NANOS_TZ:
          if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
              && logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
            LogicalTypeAnnotation.TimestampLogicalTypeAnnotation annotation =
                (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
            boolean micros = annotation.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
            boolean nanos = annotation.getUnit() == LogicalTypeAnnotation.TimeUnit.NANOS;
            boolean adjustedToUTC = annotation.isAdjustedToUTC();
            return (variantType == Variant.Type.TIMESTAMP_TZ && micros && adjustedToUTC)
                || (variantType == Variant.Type.TIMESTAMP_NTZ && micros && !adjustedToUTC)
                || (variantType == Variant.Type.TIMESTAMP_NANOS_TZ && nanos && adjustedToUTC)
                || (variantType == Variant.Type.TIMESTAMP_NANOS_NTZ && nanos && !adjustedToUTC);
          } else {
            return false;
          }
        case STRING:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
              && logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation;
        case BINARY:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY && logicalType == null;
        default:
          return false;
      }
    } else if (typedValueField.getLogicalTypeAnnotation()
        instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
      return variantType == Variant.Type.ARRAY;
    } else {
      return variantType == Variant.Type.OBJECT;
    }
  }

  private void writeScalarValue(Variant variant) {
    switch (variant.getType()) {
      case BOOLEAN:
        recordConsumer.addBoolean(variant.getBoolean());
        break;
      case BYTE:
        recordConsumer.addInteger(variant.getByte());
        break;
      case SHORT:
        recordConsumer.addInteger(variant.getShort());
        break;
      case INT:
        recordConsumer.addInteger(variant.getInt());
        break;
      case LONG:
        recordConsumer.addLong(variant.getLong());
        break;
      case FLOAT:
        recordConsumer.addFloat(variant.getFloat());
        break;
      case DOUBLE:
        recordConsumer.addDouble(variant.getDouble());
        break;
      case DECIMAL4:
        recordConsumer.addInteger(variant.getDecimal().unscaledValue().intValue());
        break;
      case DECIMAL8:
        recordConsumer.addLong(variant.getDecimal().unscaledValue().longValue());
        break;
      case DECIMAL16:
        recordConsumer.addBinary(Binary.fromConstantByteArray(
            variant.getDecimal().unscaledValue().toByteArray()));
        break;
      case DATE:
        recordConsumer.addInteger(variant.getInt());
        break;
      case TIME:
        recordConsumer.addLong(variant.getLong());
        break;
      case TIMESTAMP_TZ:
        recordConsumer.addLong(variant.getLong());
        break;
      case TIMESTAMP_NTZ:
        recordConsumer.addLong(variant.getLong());
        break;
      case TIMESTAMP_NANOS_TZ:
        recordConsumer.addLong(variant.getLong());
        break;
      case TIMESTAMP_NANOS_NTZ:
        recordConsumer.addLong(variant.getLong());
        break;
      case STRING:
        recordConsumer.addBinary(Binary.fromString(variant.getString()));
        break;
      case BINARY:
        recordConsumer.addBinary(Binary.fromReusedByteBuffer(variant.getBinary()));
        break;
      default:
        throw new IllegalArgumentException("Unsupported scalar type: " + variant.getType());
    }
  }

  private void writeArrayValue(Variant variant, GroupType arrayType) {
    Preconditions.checkArgument(
        variant.getType() == Variant.Type.ARRAY,
        "Cannot write variant type " + variant.getType() + " as array");

    // Validate that it's a 3-level array.
    if (arrayType.getFieldCount() != 1
        || arrayType.getRepetition() == Type.Repetition.REPEATED
        || arrayType.getType(0).isPrimitive()
        || !arrayType.getFieldName(0).equals(LIST_REPEATED_NAME)) {
      throw new IllegalArgumentException("Variant list must be a three-level list structure: " + arrayType);
    }

    // Get the element type from the array schema
    GroupType repeatedType = arrayType.getType(0).asGroupType();

    if (repeatedType.getFieldCount() != 1
        || repeatedType.getRepetition() != Type.Repetition.REPEATED
        || repeatedType.getType(0).isPrimitive()
        || !repeatedType.getFieldName(0).equals(LIST_ELEMENT_NAME)) {
      throw new IllegalArgumentException("Variant list must be a three-level list structure: " + arrayType);
    }

    GroupType elementType = repeatedType.getType(0).asGroupType();

    // List field, annotated as LIST
    recordConsumer.startGroup();
    int numElements = variant.numArrayElements();
    // Can only call startField if there is at least one element.
    if (numElements > 0) {
      recordConsumer.startField(LIST_REPEATED_NAME, 0);
      // Write each array element
      for (int i = 0; i < numElements; i++) {
        // Repeated group.
        recordConsumer.startGroup();
        recordConsumer.startField(LIST_ELEMENT_NAME, 0);

        // Element group. Can never be null for shredded Variant.
        recordConsumer.startGroup();
        write(elementType, variant.getElementAtIndex(i));
        recordConsumer.endGroup();

        recordConsumer.endField(LIST_ELEMENT_NAME, 0);
        recordConsumer.endGroup();
      }
      recordConsumer.endField(LIST_REPEATED_NAME, 0);
    }
    recordConsumer.endGroup();
  }

  /**
   * Write an object to typed_value
   *
   * @return the residual value that must be written to the value column, or null if all values were written
   *         to typed_value.
   */
  private ByteBuffer writeObjectValue(Variant variant, GroupType objectType) {
    Preconditions.checkArgument(
        variant.getType() == Variant.Type.OBJECT,
        "Cannot write variant type " + variant.getType() + " as object");

    VariantBuilder residualBuilder = null;
    // The residualBuilder, if created, is always a single object. This is that object's builder.
    VariantObjectBuilder objectBuilder = null;

    // Write each object field.
    recordConsumer.startGroup();
    for (int i = 0; i < variant.numObjectElements(); i++) {
      Variant.ObjectField field = variant.getFieldAtIndex(i);

      if (objectType.containsField(field.key)) {
        int fieldIndex = objectType.getFieldIndex(field.key);
        Type fieldType = objectType.getType(fieldIndex);

        recordConsumer.startField(field.key, fieldIndex);
        recordConsumer.startGroup();
        write(fieldType.asGroupType(), field.value);
        recordConsumer.endGroup();
        recordConsumer.endField(field.key, objectType.getFieldIndex(field.key));
      } else {
        if (residualBuilder == null) {
          residualBuilder = new VariantBuilder(getMetadata());
          objectBuilder = residualBuilder.startObject();
        }
        objectBuilder.appendKey(field.key);
        objectBuilder.appendEncodedValue(field.value.getValueBuffer());
      }
    }
    recordConsumer.endGroup();

    if (residualBuilder != null) {
      residualBuilder.endObject();
      return residualBuilder.build().getValueBuffer();
    } else {
      return null;
    }
  }
}
