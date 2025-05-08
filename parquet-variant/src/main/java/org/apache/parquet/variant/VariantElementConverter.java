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

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Converter for a shredded Variant containing a value and/or typed_value field: either a top-level
 * Variant column, or a nested array element or object field. The top-level converter is handled
 * by a subclass (VariantColumnConverter) that also reads metadata.
 *
 * Converters for the `value` and `typed_value` fields are implemented as nested classes in this
 * class.
 *
 * All converters for a Variant column append their results to a VariantBuilder as values are read from Parquet.
 *
 * Values in `typed_value` are appended by the child converter. Values in `value` are stored by a
 * child converter, but only appended when completing this group. Additionally, object fields are
 * appended by the `typed_value` converter, but because residual values are stored in `value`, this
 * converter is responsible for finalizing the object.
 */
class VariantElementConverter extends GroupConverter implements VariantConverter {

  // We need to remember the start position in order to tell if the child typed_value was non-null.
  // Maybe we should just add a callback to each child's end()?
  private int startWritePos;
  private boolean typedValueIsObject = false;
  private int valueIdx = -1;
  private int typedValueIdx = -1;
  protected VariantBuilderHolder holder;
  protected Converter[] converters;

  // The following are only used if this is an object field.
  private String objectFieldName = null;
  private int objectFieldId = -1;
  private VariantObjectConverter parent = null;

  // Only used if typedValueIsObject is true.
  private Set<String> shreddedObjectKeys;

  @Override
  public void init(VariantBuilderHolder holder) {
    this.holder = holder;
    for (Converter converter : converters) {
      if (converter != null) {
        ((VariantConverter) converter).init(holder);
      }
    }
  }

  public VariantElementConverter(GroupType variantSchema, String objectFieldName, VariantObjectConverter parent) {
    this(variantSchema);
    this.objectFieldName = objectFieldName;
    this.parent = parent;
  }

  public VariantElementConverter(GroupType variantSchema) {
    converters = new Converter[variantSchema.getFieldCount()];

    List<Type> fields = variantSchema.getFields();

    for (int i = 0; i < fields.size(); i++) {
      Type field = fields.get(i);
      String fieldName = field.getName();
      if (fieldName.equals("value")) {
        this.valueIdx = i;
        if (!field.isPrimitive() || field.asPrimitiveType().getPrimitiveTypeName() != BINARY) {
          throw new IllegalArgumentException("Value must be a binary value");
        }
      } else if (fieldName.equals("typed_value")) {
        this.typedValueIdx = i;
      }
    }

    if (valueIdx >= 0) {
      converters[valueIdx] = new VariantValueConverter(this);
    }

    if (typedValueIdx >= 0) {
      Converter typedConverter = null;
      Type field = fields.get(typedValueIdx);
      LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
      if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
        typedConverter = new VariantArrayConverter(field.asGroupType());
      } else if (!field.isPrimitive()) {
        GroupType typedValue = field.asGroupType();
        typedConverter = new VariantObjectConverter(typedValue);
        typedValueIsObject = true;
        shreddedObjectKeys = new HashSet<>();
        for (Type f : typedValue.getFields()) {
          shreddedObjectKeys.add(f.getName());
        }
      } else {
        typedConverter = VariantScalarConverter.create(field.asPrimitiveType());
      }

      assert (typedConverter != null);
      converters[typedValueIdx] = typedConverter;
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  /** runtime calls  **/

  @Override
  public void start() {
    if (objectFieldName != null) {
      // Having a non-null element does not guarantee that we actually want to add this field, because
      // if value and typed_value are both null, it's a missing field. In that case, we'll detect it
      // in end() and reverse our decision to add this key.
      ((VariantObjectBuilder) holder.builder).appendKey(objectFieldName);
    }
    if (valueIdx >= 0) {
      ((VariantValueConverter) converters[valueIdx]).reset();
    }
    startWritePos = holder.builder.getWritePos();
  }

  @Override
  public void end() {
    VariantBuilder builder = this.holder.builder;

    Binary variantValue = null;
    VariantObjectBuilder objectBuilder = null;
    if (typedValueIsObject) {
      // Get the builder that the child typed_value has been adding its fields to. We need to possibly add
      // more values from the `value` field, then finalize. If the value was not an object, fields will be null.
      objectBuilder = ((VariantObjectConverter) converters[typedValueIdx]).getObjectBuilder();
    }

    // If objectBuilder is non-null, then we have a partially complete object ready to write.
    // Otherwise, the typed_value converter should have written something if it was non-null.
    boolean hasTypedValue = objectBuilder != null || (startWritePos != builder.getWritePos());

    if (valueIdx >= 0) {
      variantValue = ((VariantValueConverter) converters[valueIdx]).getValue();
    }
    if (variantValue != null) {
      if (!hasTypedValue) {
        // Nothing else was added. We can directly append this value.
        builder.shallowAppendVariant(variantValue.toByteBuffer());
      } else {
        // Both value and typed_value were non-null. This is only valid for an object.
        Variant value = new Variant(variantValue.toByteBuffer(), this.holder.topLevelHolder.metadata.toByteBuffer());
        Variant.Type basicType = value.getType();
        if (hasTypedValue && basicType != Variant.Type.OBJECT) {
          throw new IllegalArgumentException("Invalid variant, conflicting value and typed_value");
        }

        for (int i = 0; i < value.numObjectElements(); i++) {
          Variant.ObjectField field = value.getFieldAtIndex(i);
          if (shreddedObjectKeys.contains(field.key)) {
            // Skip any field ID that is also in the typed schema. This check is needed because readers with
            // pushdown may not look at the value column, causing inconsistent results if a writer puth a given key
            // only in the value column when it was present in the typed_value schema.
            // Alternatively, we could fail at this point, since the shredding is invalid according to the spec.
            continue;
          }
          objectBuilder.appendKey(field.key);
          objectBuilder.shallowAppendVariant(field.value.getValueRawBytes());
        }
        builder.endObject();
      }
    } else if (objectBuilder != null) {
      // typed_value was an object, and there's nothing left to append.
      builder.endObject();
    } else if (!hasTypedValue) {
      // There was no value or typed_value.
      if (objectFieldName != null) {
        // Missing field.
        ((VariantObjectBuilder) this.holder.builder).dropLastKey();
      } else {
        // For arrays and top-level fields, the spec considers this invalid, but recommends using VariantNull.
        builder.appendNull();
      }
    }
  }

  /**
   * Converter for the metadata column. It sets the current metadata in the parent converter,
   * so that it can be used by the typed_value converter on the same row.
   */
  static class VariantMetadataConverter extends PrimitiveConverter implements VariantConverter {
    private VariantBuilderTopLevelHolder builder;
    Binary[] dict;

    public VariantMetadataConverter() {
      dict = null;
    }

    @Override
    public void init(VariantBuilderHolder builderHolder) {
      builder = (VariantBuilderTopLevelHolder) builderHolder;
    }

    @Override
    public void addBinary(Binary value) {
      builder.setMetadata(value);
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      dict = new Binary[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        dict[i] = dictionary.decodeToBinary(i);
      }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      builder.setMetadata(dict[dictionaryId]);
    }
  }

  // Converter for the `value` field. It does not append to VariantBuilder directly: it simply holds onto
  // its value for the parent converter to append.
  static class VariantValueConverter extends PrimitiveConverter implements VariantConverter {
    private VariantElementConverter parent;
    Binary[] dict;
    Binary currentValue;

    public VariantValueConverter(VariantElementConverter parent) {
      this.parent = parent;
      this.currentValue = null;
      dict = null;
    }

    @Override
    public void init(VariantBuilderHolder builderHolder) {}

    void reset() {
      currentValue = null;
    }

    Binary getValue() {
      return currentValue;
    }

    @Override
    public void addBinary(Binary value) {
      currentValue = value;
    }

    @Override
    public boolean hasDictionarySupport() {
      return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      dict = new Binary[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        dict[i] = dictionary.decodeToBinary(i);
      }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      currentValue = dict[dictionaryId];
    }
  }

  // Base class for converting primitive typed_value fields.
  static class VariantScalarConverter extends PrimitiveConverter implements VariantConverter {
    protected VariantBuilderHolder builder;
    private GroupType scalarType;

    @Override
    public void init(VariantBuilderHolder builderHolder) {
      builder = builderHolder;
    }

    // Return an appropriate converter for the given Parquet type.
    static VariantScalarConverter create(PrimitiveType primitive) {
      VariantScalarConverter typedConverter = null;
      LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
      PrimitiveType.PrimitiveTypeName primitiveType = primitive.getPrimitiveTypeName();
      if (primitiveType == BOOLEAN) {
        typedConverter = new VariantBooleanConverter();
      } else if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
            (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
        if (!intAnnotation.isSigned()) {
          throw new UnsupportedOperationException("Unsupported shredded value type: " + intAnnotation);
        }
        int width = intAnnotation.getBitWidth();
        if (width == 8) {
          typedConverter = new VariantByteConverter();
        } else if (width == 16) {
          typedConverter = new VariantShortConverter();
        } else if (width == 32) {
          typedConverter = new VariantIntConverter();
        } else if (width == 64) {
          typedConverter = new VariantLongConverter();
        } else {
          throw new UnsupportedOperationException("Unsupported shredded value type: " + intAnnotation);
        }
      } else if (annotation == null && primitiveType == INT32) {
        typedConverter = new VariantIntConverter();
      } else if (annotation == null && primitiveType == INT64) {
        typedConverter = new VariantLongConverter();
      } else if (primitiveType == FLOAT) {
        typedConverter = new VariantFloatConverter();
      } else if (primitiveType == DOUBLE) {
        typedConverter = new VariantDoubleConverter();
      } else if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
            (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
        typedConverter = new VariantDecimalConverter(decimalType.getScale());
      } else if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
        typedConverter = new VariantDateConverter();
      } else if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
            (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
        if (timestampType.isAdjustedToUTC()) {
          switch (timestampType.getUnit()) {
            case MILLIS:
              throw new UnsupportedOperationException("MILLIS not supported by Variant");
            case MICROS:
              typedConverter = new VariantTimestampConverter();
              break;
            case NANOS:
              typedConverter = new VariantTimestampNanosConverter();
              break;
          }
        } else {
          switch (timestampType.getUnit()) {
            case MILLIS:
              throw new UnsupportedOperationException("MILLIS not supported by Variant");
            case MICROS:
              typedConverter = new VariantTimestampNtzConverter();
              break;
            case NANOS:
              typedConverter = new VariantTimestampNanosNtzConverter();
              break;
          }
        }
      } else if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType =
            (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation;
        if (timeType.isAdjustedToUTC() || timeType.getUnit() != MICROS) {
          throw new UnsupportedOperationException(timeType + " not supported by Variant");
        } else {
          typedConverter = new VariantTimeConverter();
        }
      } else if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
        typedConverter = new VariantUuidConverter();
      } else if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
        typedConverter = new VariantStringConverter();
      } else if (primitiveType == BINARY) {
        typedConverter = new VariantBinaryConverter();
      } else {
        throw new UnsupportedOperationException("Unsupported shredded value type: " + primitive);
      }
      return typedConverter;
    }
  }

  static class VariantStringConverter extends VariantScalarConverter {
    @Override
    public void addBinary(Binary value) {
      builder.builder.appendString(value.toStringUsingUTF8());
    }
  }

  static class VariantBinaryConverter extends VariantScalarConverter {
    @Override
    public void addBinary(Binary value) {
      builder.builder.appendBinary(value.toByteBuffer());
    }
  }

  static class VariantDecimalConverter extends VariantScalarConverter {
    private int scale;

    VariantDecimalConverter(int scale) {
      super();
      this.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      builder.builder.appendDecimal(new BigDecimal(new BigInteger(value.getBytes()), scale));
    }

    @Override
    public void addLong(long value) {
      BigDecimal decimal = BigDecimal.valueOf(value, scale);
      builder.builder.appendDecimal(decimal);
    }

    @Override
    public void addInt(int value) {
      BigDecimal decimal = BigDecimal.valueOf(value, scale);
      builder.builder.appendDecimal(decimal);
    }
  }

  static class VariantUuidConverter extends VariantScalarConverter {
    @Override
    public void addBinary(Binary value) {
      builder.builder.appendUUIDBytes(value.toByteBuffer());
    }
  }

  static class VariantBooleanConverter extends VariantScalarConverter {
    @Override
    public void addBoolean(boolean value) {
      builder.builder.appendBoolean(value);
    }
  }

  static class VariantDoubleConverter extends VariantScalarConverter {
    @Override
    public void addDouble(double value) {
      builder.builder.appendDouble(value);
    }
  }

  static class VariantFloatConverter extends VariantScalarConverter {
    @Override
    public void addFloat(float value) {
      builder.builder.appendFloat(value);
    }
  }

  static class VariantByteConverter extends VariantScalarConverter {
    @Override
    public void addInt(int value) {
      builder.builder.appendByte((byte) value);
    }
  }

  static class VariantShortConverter extends VariantScalarConverter {
    @Override
    public void addInt(int value) {
      builder.builder.appendShort((short) value);
    }
  }

  static class VariantIntConverter extends VariantScalarConverter {
    @Override
    public void addInt(int value) {
      builder.builder.appendInt(value);
    }
  }

  static class VariantLongConverter extends VariantScalarConverter {
    @Override
    public void addLong(long value) {
      builder.builder.appendLong(value);
    }
  }

  static class VariantDateConverter extends VariantScalarConverter {
    @Override
    public void addInt(int value) {
      builder.builder.appendDate(value);
    }
  }

  static class VariantTimeConverter extends VariantScalarConverter {
    @Override
    public void addLong(long value) {
      builder.builder.appendTime(value);
    }
  }

  static class VariantTimestampConverter extends VariantScalarConverter {
    @Override
    public void addLong(long value) {
      builder.builder.appendTimestampTz(value);
    }
  }

  static class VariantTimestampNtzConverter extends VariantScalarConverter {
    @Override
    public void addLong(long value) {
      builder.builder.appendTimestampNtz(value);
    }
  }

  static class VariantTimestampNanosConverter extends VariantScalarConverter {
    @Override
    public void addLong(long value) {
      builder.builder.appendTimestampNanosTz(value);
    }
  }

  static class VariantTimestampNanosNtzConverter extends VariantScalarConverter {
    @Override
    public void addLong(long value) {
      builder.builder.appendTimestampNanosNtz(value);
    }
  }

  /**
   * Converter for a LIST typed_value.
   */
  static class VariantArrayConverter extends GroupConverter implements VariantConverter {
    private VariantBuilderHolder builder;
    private VariantArrayRepeatedConverter repeatedConverter;

    public VariantArrayConverter(GroupType listType) {
      if (listType.getFieldCount() != 1) {
        throw new IllegalArgumentException("LIST must have one field");
      }
      Type middleLevel = listType.getType(0);
      if (!middleLevel.isRepetition(REPEATED) || middleLevel.isPrimitive()
          || middleLevel.asGroupType().getFieldCount() != 1) {
        throw new IllegalArgumentException("LIST must have one repeated field");
      }
      this.repeatedConverter = new VariantArrayRepeatedConverter(middleLevel.asGroupType(), this);
    }

    @Override
    public void init(VariantBuilderHolder builderHolder) {
      // Create a new builder for the array.
      builder = new VariantBuilderHolder(builderHolder);
      repeatedConverter.init(builder);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return repeatedConverter;
    }

    @Override
    public void start() {
      builder.startNewArray();
    }

    @Override
    public void end() {
      builder.parentHolder.builder.endArray();
    }
  }

  /**
   * Converter for the repeated field of a LIST typed_value.
   */
  static class VariantArrayRepeatedConverter extends GroupConverter implements VariantConverter {
    private VariantElementConverter elementConverter;

    public VariantArrayRepeatedConverter(GroupType repeatedType, VariantArrayConverter parentaConverter) {
      this.elementConverter = new VariantElementConverter(repeatedType.getType(0).asGroupType());
    }

    @Override
    public void init(VariantBuilderHolder builderHolder) {
      elementConverter.init(builderHolder);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return elementConverter;
    }

    @Override
    public void start() {}

    @Override
    public void end() {}
  }

  static class VariantObjectConverter extends GroupConverter implements VariantConverter {
    private VariantBuilderHolder builder;
    private VariantElementConverter[] converters;

    public VariantObjectConverter(GroupType typed_value) {
      List<Type> fields = typed_value.getFields();
      converters = new VariantElementConverter[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        GroupType field = fields.get(i).asGroupType();
        String name = fields.get(i).getName();
        converters[i] = new VariantElementConverter(field, name, this);
      }
    }

    /**
     * This method must be called after each call to end() to ensure that the object builder is
     * not reused on a subsequent value for which start() was never called.
     *
     * @return The partially built object builder, or null if no object was constructed.
     */
    VariantObjectBuilder getObjectBuilder() {
      VariantObjectBuilder objectBuilder = (VariantObjectBuilder) builder.builder;
      builder.builder = null;
      return objectBuilder;
    }

    @Override
    public void init(VariantBuilderHolder builderHolder) {
      // Create a new builder for the object.
      builder = new VariantBuilderHolder(builderHolder);
      for (VariantElementConverter c : converters) {
        c.init(builder);
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      builder.startNewObject();
    }

    @Override
    public void end() {
      // We can't finish writing the object here, because there might be residual entries in our
      // parent's value column. The parent converter calls getObjectBuilder to finalize the object.
    }
  }
}
