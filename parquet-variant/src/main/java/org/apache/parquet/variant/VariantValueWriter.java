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

import java.nio.ByteBuffer;
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
  private ByteBuffer metadataBuffer;
  // We defer initializing the ImmutableMetata until it's needed. It has some construction cost, and if all
  // object fields are shredded into typed_value, it will never be used.
  private ImmutableMetadata metadata = null;
  private RecordConsumer recordConsumer;

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
    if (isTypeCompatible(variantType, typedValueField)) {
      int typedValueIdx = schema.getFieldIndex("typed_value");
      recordConsumer.startField("typed_value", typedValueIdx);
      ByteBuffer residual = null;
      if (typedValueField.isPrimitive()) {
        writeScalarValue(recordConsumer, value, typedValueField.asPrimitiveType());
      } else if (typedValueField.getLogicalTypeAnnotation()
          instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
        writeArrayValue(recordConsumer, value, typedValueField.asGroupType());
      } else {
        residual = writeObjectValue(recordConsumer, value, typedValueField.asGroupType());
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

  private boolean isTypeCompatible(Variant.Type variantType, Type typedValueField) {
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
              && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).getBitWidth() == 8;
        case SHORT:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
              && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType).getBitWidth() == 16;
        case INT:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && (logicalType == null
                  || (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation
                      && ((LogicalTypeAnnotation.IntLogicalTypeAnnotation) logicalType)
                              .getBitWidth()
                          == 32));
        case LONG:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
              && (logicalType == null
                  || logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation);
        case FLOAT:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT;
        case DOUBLE:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE;
        case DECIMAL4:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32
              && logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
        case DECIMAL8:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64
              && logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
        case DECIMAL16:
          return primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
              && logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
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

  private void writeScalarValue(RecordConsumer recordConsumer, Variant variant, PrimitiveType type) {
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

  private void writeArrayValue(RecordConsumer recordConsumer, Variant variant, GroupType arrayType) {
    if (variant.getType() != Variant.Type.ARRAY) {
      throw new IllegalArgumentException("Expected array type but got: " + variant.getType());
    }

    // Get the element type from the array schema
    GroupType listType = arrayType.getType(0).asGroupType();
    Type elementType = listType.getType(0);

    recordConsumer.startGroup();
    recordConsumer.startField(arrayType.getFieldName(0), 0);
    // Write each array element
    for (int i = 0; i < variant.numArrayElements(); i++) {
      recordConsumer.startGroup();
      recordConsumer.startField("element", 0);

      recordConsumer.startGroup();
      write(elementType.asGroupType(), variant.getElementAtIndex(i));
      recordConsumer.endGroup();

      recordConsumer.endField("element", 0);
      recordConsumer.endGroup();
    }
    recordConsumer.endField(arrayType.getFieldName(0), 0);
    recordConsumer.endGroup();
  }

  /**
   * Write an object to typed_value
   *
   * @return the residual value that must be written to the value column, or null if all values were written
   *         to typed_value.
   */
  private ByteBuffer writeObjectValue(RecordConsumer recordConsumer, Variant variant, GroupType objectType) {
    if (variant.getType() != Variant.Type.OBJECT) {
      throw new IllegalArgumentException("Expected object type but got: " + variant.getType());
    }

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
