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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class VariantConverters {
  // do not allow instantiating this class
  private VariantConverters() {
  }

  public interface ParentConverter<T extends VariantBuilder> {
    void build(Consumer<T> buildConsumer);
  }

  public static GroupConverter newVariantConverter(GroupType variantGroup, Consumer<ByteBuffer> metadata,
                                                   ParentConverter<VariantBuilder> parent) {
    ValueConverter converter = newValueConverter(variantGroup, parent);

    // if there is a metadata field, add a converter for it
    if (variantGroup.containsField("metadata")) {
      int metadataIndex = variantGroup.getFieldIndex("metadata");
      Type metadataField = variantGroup.getType("metadata");
      Preconditions.checkArgument(isBinary(metadataField), "Invalid metadata field: " + metadataField);
      converter.setConverter(metadataIndex, new VariantMetadataConverter(metadata));
    }

    return converter;
  }

  static ValueConverter newValueConverter(GroupType valueGroup, ParentConverter<VariantBuilder> parent) {
    if (valueGroup.containsField("typed_value")) {
      Type typedValueField = valueGroup.getType("typed_value");
      if (isShreddedObject(typedValueField)) {
        return new ShreddedObjectConverter(valueGroup, parent);
      }
    }

    return new ShreddedValueConverter(valueGroup, parent);
  }

  /**
   * Converter for metadata
   */
  static class VariantMetadataConverter extends BinaryConverter {
    private final Consumer<ByteBuffer> handleMetadata;

    public VariantMetadataConverter(Consumer<ByteBuffer> handleMetadata) {
      this.handleMetadata = handleMetadata;
    }

    @Override
    protected void handleBinary(Binary value) {
      handleMetadata.accept(value.toByteBuffer());
    }
  }

  /**
   * Converter for a non-object value field; appends the encoded value to the builder
   */
  static class VariantValueConverter extends BinaryConverter {
    private final ParentConverter<VariantBuilder> parent;

    public VariantValueConverter(ParentConverter<VariantBuilder> parent) {
      this.parent = parent;
    }

    @Override
    protected void handleBinary(Binary value) {
      parent.build(builder -> builder.appendEncodedValue(value.toByteBuffer()));
    }
  }

  static class ValueConverter extends GroupConverter {
    private final Converter[] converters;

    ValueConverter(GroupType group) {
      this.converters = new Converter[group.getFieldCount()];
    }

    protected void setConverter(int index, Converter converter) {
      this.converters[index] = converter;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
    }

    @Override
    public void end() {
    }
  }

  static class ShreddedValueConverter extends ValueConverter {
    ShreddedValueConverter(GroupType group, ParentConverter<VariantBuilder> parent) {
      super(group);

      if (group.containsField("typed_value")) {
        int typedValueIndex = group.getFieldIndex("typed_value");
        Type valueField = group.getType(typedValueIndex);
        if (valueField.isPrimitive()) {
          setConverter(typedValueIndex, newScalarConverter(valueField.asPrimitiveType(), parent));
        } else if (isShreddedArray(valueField)) {
          setConverter(typedValueIndex, new ShreddedArrayConverter(valueField.asGroupType(), parent));
        }
      }

      if (group.containsField("value")) {
        int valueIndex = group.getFieldIndex("value");
        Type typedField = group.getType(valueIndex);
        Preconditions.checkArgument(
            typedField.isPrimitive() && typedField.asPrimitiveType().getPrimitiveTypeName() == BINARY,
            "Invalid variant value type: " + typedField);

        setConverter(valueIndex, new VariantValueConverter(parent));
      }
    }
  }

  static class ShreddedObjectConverter extends ValueConverter {
    private final ParentConverter<VariantBuilder> parent;

    ShreddedObjectConverter(GroupType group, ParentConverter<VariantBuilder> parent) {
      super(group);
      this.parent = parent;

      int typedValueIndex = group.getFieldIndex("typed_value");
      Type typedField = group.getType(typedValueIndex);
      Preconditions.checkArgument(
          isShreddedObject(typedField), "Invalid typed_value for shredded object: " + typedField);

      PartiallyShreddedFieldsConverter fieldsConverter =
          new PartiallyShreddedFieldsConverter(typedField.asGroupType(), parent);
      setConverter(typedValueIndex, fieldsConverter);

      if (group.containsField("value")) {
        int valueIndex = group.getFieldIndex("value");
        Type valueField = group.getType(valueIndex);
        Preconditions.checkArgument(isBinary(valueField), "Invalid variant value type: " + valueField);

        setConverter(valueIndex, new PartiallyShreddedValueConverter(parent, fieldsConverter.shreddedFieldNames()));
      }
    }

    @Override
    public void end() {
      parent.build(VariantBuilder::endObjectIfExists);
    }
  }

  /**
   * Converter for a partially shredded object's value field; copies object fields or appends an encoded non-object.
   *
   * <p>This must be wrapped by {@link ShreddedObjectConverter} to finalize the object.
   */
  static class PartiallyShreddedValueConverter extends BinaryConverter {
    private final ParentConverter<VariantBuilder> parent;
    private final Set<String> suppressedKeys;

    public PartiallyShreddedValueConverter(ParentConverter<VariantBuilder> parent, Set<String> suppressedKeys) {
      this.parent = parent;
      this.suppressedKeys = suppressedKeys;
    }

    @Override
    protected void handleBinary(Binary value) {
      parent.build(builder -> {
        ByteBuffer buffer = value.toByteBuffer();
        if (VariantUtil.getType(buffer) == Variant.Type.OBJECT) {
          builder.startOrContinuePartialObject(value.toByteBuffer(), suppressedKeys);
        } else {
          builder.appendEncodedValue(buffer);
        }
      });
    }
  }

  /**
   * Converter for a shredded object's shredded fields.
   *
   * <p>This must be wrapped by {@link ShreddedObjectConverter} to finalize the object.
   */
  static class PartiallyShreddedFieldsConverter extends GroupConverter {
    private final Converter[] converters;
    private final ParentConverter<VariantBuilder> parent;
    private final Set<String> shreddedFieldNames = new HashSet<>();
    private VariantObjectBuilder objectBuilder = null;

    PartiallyShreddedFieldsConverter(GroupType fieldsType, ParentConverter<VariantBuilder> parent) {
      this.converters = new Converter[fieldsType.getFieldCount()];
      this.parent = parent;

      for (int index = 0; index < fieldsType.getFieldCount(); index += 1) {
        Type field = fieldsType.getType(index);
        Preconditions.checkArgument(!field.isPrimitive(), "Invalid field group: " + field);

        String name = field.getName();
        shreddedFieldNames.add(name);
        ParentConverter<VariantObjectBuilder> newParent = converter -> converter.accept(objectBuilder);
        converters[index] = new FieldValueConverter(name, field.asGroupType(), newParent);
      }
    }

    private Set<String> shreddedFieldNames() {
      return shreddedFieldNames;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      try {
        // the builder may already have an object builder from the value field
        parent.build(builder -> this.objectBuilder = builder.startOrContinueObject());
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Invalid variant, conflicting value and typed_value", e);
      }
    }

    @Override
    public void end() {
      this.objectBuilder = null;
    }

    private static class FieldValueConverter extends GroupConverter {
      private final String fieldName;
      private final GroupConverter converter;
      private final ParentConverter<VariantObjectBuilder> parent;
      private Long numFields = null;

      FieldValueConverter(String fieldName, GroupType field, ParentConverter<VariantObjectBuilder> parent) {
        this.fieldName = fieldName;
        this.converter = newValueConverter(field.asGroupType(), consumer -> parent.build(consumer::accept));
        this.parent = parent;
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        return converter.getConverter(fieldIndex);
      }

      @Override
      public void start() {
        converter.start();
        parent.build(builder -> {
          this.numFields = builder.numValues;
          builder.appendKey(fieldName);
        });
      }

      @Override
      public void end() {
        parent.build(builder -> {
          if (builder.numValues == numFields) {
            builder.dropLastKey();
          }
        });
        this.numFields = null;
        converter.end();
      }
    }
  }

  /**
   * Converter for a shredded array
   */
  static class ShreddedArrayConverter extends GroupConverter {
    private final ParentConverter<? extends VariantBuilder> parent;
    private final ShreddedArrayRepeatedConverter repeatedConverter;
    private VariantArrayBuilder arrayBuilder;

    public ShreddedArrayConverter(GroupType list, ParentConverter<? extends VariantBuilder> parent) {
      Preconditions.checkArgument(list.getFieldCount() == 1, "Invalid list type: " + list);
      this.parent = parent;

      Type repeated = list.getType(0);
      Preconditions.checkArgument(
          repeated.isRepetition(REPEATED) && !repeated.isPrimitive() && repeated.asGroupType().getFieldCount() == 1,
          "Invalid repeated type in list: " + repeated);

      this.repeatedConverter = new ShreddedArrayRepeatedConverter(
          repeated.asGroupType(), consumer -> consumer.accept(arrayBuilder));
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 1) {
        throw new ArrayIndexOutOfBoundsException(fieldIndex);
      }

      return repeatedConverter;
    }

    @Override
    public void start() {
      try {
        parent.build(builder -> this.arrayBuilder = builder.startArray());
      } catch (IllegalStateException e) {
        throw new IllegalStateException("Invalid variant, conflicting value and typed_value", e);
      }
    }

    @Override
    public void end() {
      parent.build(VariantBuilder::endArray);
      this.arrayBuilder = null;
    }
  }

  /**
   * Converter for the repeated field of a shredded array
   */
  static class ShreddedArrayRepeatedConverter extends GroupConverter {
    private final GroupConverter elementConverter;
    private final ParentConverter<VariantArrayBuilder> parent;
    private Long numValues = null;

    public ShreddedArrayRepeatedConverter(
        GroupType repeated, ParentConverter<VariantArrayBuilder> parent) {
      Type element = repeated.getType(0);
      Preconditions.checkArgument(
          !element.isPrimitive(), "Invalid element type in variant array: " + element);

      ParentConverter<VariantBuilder> newParent = consumer -> parent.build(consumer::accept);
      this.elementConverter = newValueConverter(element.asGroupType(), newParent);
      this.parent = parent;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      if (fieldIndex > 1) {
        throw new ArrayIndexOutOfBoundsException(fieldIndex);
      }

      return elementConverter;
    }

    @Override
    public void start() {
      parent.build(builder -> this.numValues = builder.numValues());
    }

    @Override
    public void end() {
      parent.build(builder -> {
        long valuesWritten = builder.numValues() - this.numValues;
        if (valuesWritten > 1) {
          throw new IllegalStateException("Invalid variant, conflicting value and typed_value");
        } else if (valuesWritten == 0) {
          builder.appendNull();
        }
      });
    }
  }

  // Return an appropriate converter for the given Parquet type.
  static Converter newScalarConverter(PrimitiveType primitive, ParentConverter<VariantBuilder> parent) {
    ShreddedScalarConverter typedConverter = null;
    LogicalTypeAnnotation annotation = primitive.getLogicalTypeAnnotation();
    PrimitiveType.PrimitiveTypeName primitiveType = primitive.getPrimitiveTypeName();
    if (primitiveType == BOOLEAN) {
      typedConverter = new VariantBooleanConverter(parent);
    } else if (annotation instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
      LogicalTypeAnnotation.IntLogicalTypeAnnotation intAnnotation =
          (LogicalTypeAnnotation.IntLogicalTypeAnnotation) annotation;
      if (!intAnnotation.isSigned()) {
        throw new UnsupportedOperationException("Unsupported shredded value type: " + intAnnotation);
      }
      int width = intAnnotation.getBitWidth();
      if (width == 8) {
        typedConverter = new VariantByteConverter(parent);
      } else if (width == 16) {
        typedConverter = new VariantShortConverter(parent);
      } else if (width == 32) {
        typedConverter = new VariantIntConverter(parent);
      } else if (width == 64) {
        typedConverter = new VariantLongConverter(parent);
      } else {
        throw new UnsupportedOperationException("Unsupported shredded value type: " + intAnnotation);
      }
    } else if (annotation == null && primitiveType == INT32) {
      typedConverter = new VariantIntConverter(parent);
    } else if (annotation == null && primitiveType == INT64) {
      typedConverter = new VariantLongConverter(parent);
    } else if (primitiveType == FLOAT) {
      typedConverter = new VariantFloatConverter(parent);
    } else if (primitiveType == DOUBLE) {
      typedConverter = new VariantDoubleConverter(parent);
    } else if (annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType =
          (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) annotation;
      typedConverter = new VariantDecimalConverter(parent, decimalType.getScale());
    } else if (annotation instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
      typedConverter = new VariantDateConverter(parent);
    } else if (annotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) annotation;
      if (timestampType.isAdjustedToUTC()) {
        switch (timestampType.getUnit()) {
          case MILLIS:
            throw new UnsupportedOperationException("MILLIS not supported by Variant");
          case MICROS:
            typedConverter = new VariantTimestampConverter(parent);
            break;
          case NANOS:
            typedConverter = new VariantTimestampNanosConverter(parent);
            break;
        }
      } else {
        switch (timestampType.getUnit()) {
          case MILLIS:
            throw new UnsupportedOperationException("MILLIS not supported by Variant");
          case MICROS:
            typedConverter = new VariantTimestampNtzConverter(parent);
            break;
          case NANOS:
            typedConverter = new VariantTimestampNanosNtzConverter(parent);
            break;
        }
      }
    } else if (annotation instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType =
          (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) annotation;
      if (timeType.isAdjustedToUTC() || timeType.getUnit() != MICROS) {
        throw new UnsupportedOperationException(timeType + " not supported by Variant");
      } else {
        typedConverter = new VariantTimeConverter(parent);
      }
    } else if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
      typedConverter = new VariantUUIDConverter(parent);
    } else if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
      typedConverter = new VariantStringConverter(parent);
    } else if (primitiveType == BINARY) {
      typedConverter = new VariantBinaryConverter(parent);
    } else {
      throw new UnsupportedOperationException("Unsupported shredded value type: " + primitive);
    }
    return typedConverter;
  }

  // Base class for converting primitive typed_value fields.
  static class ShreddedScalarConverter extends PrimitiveConverter {
    protected ParentConverter<VariantBuilder> parent;

    ShreddedScalarConverter(ParentConverter<VariantBuilder> parent) {
      this.parent = parent;
    }
  }

  static class VariantStringConverter extends ShreddedScalarConverter {
    VariantStringConverter(ParentConverter<VariantBuilder> parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      parent.build(builder -> builder.appendString(value.toStringUsingUTF8()));
    }
  }

  static class VariantBinaryConverter extends ShreddedScalarConverter {
    VariantBinaryConverter(ParentConverter<VariantBuilder> parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      parent.build(builder -> builder.appendBinary(value.toByteBuffer()));
    }
  }

  static class VariantDecimalConverter extends ShreddedScalarConverter {
    private int scale;

    VariantDecimalConverter(ParentConverter<VariantBuilder> parent, int scale) {
      super(parent);
      this.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      parent.build(builder -> builder.appendDecimal(new BigDecimal(new BigInteger(value.getBytes()), scale)));
    }

    @Override
    public void addLong(long value) {
      BigDecimal decimal = BigDecimal.valueOf(value, scale);
      parent.build(builder -> builder.appendDecimal(decimal));
    }

    @Override
    public void addInt(int value) {
      BigDecimal decimal = BigDecimal.valueOf(value, scale);
      parent.build(builder -> builder.appendDecimal(decimal));
    }
  }

  static class VariantUUIDConverter extends ShreddedScalarConverter {
    VariantUUIDConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addBinary(Binary value) {
      parent.build(builder -> builder.appendUUIDBytes(value.toByteBuffer()));
    }
  }

  static class VariantBooleanConverter extends ShreddedScalarConverter {
    VariantBooleanConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addBoolean(boolean value) {
      parent.build(builder -> builder.appendBoolean(value));
    }
  }

  static class VariantDoubleConverter extends ShreddedScalarConverter {
    VariantDoubleConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addDouble(double value) {
      parent.build(builder -> builder.appendDouble(value));
    }
  }

  static class VariantFloatConverter extends ShreddedScalarConverter {
    VariantFloatConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addFloat(float value) {
      parent.build(builder -> builder.appendFloat(value));
    }
  }

  static class VariantByteConverter extends ShreddedScalarConverter {
    VariantByteConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addInt(int value) {
      parent.build(builder -> builder.appendByte((byte) value));
    }
  }

  static class VariantShortConverter extends ShreddedScalarConverter {
    VariantShortConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addInt(int value) {
      parent.build(builder -> builder.appendShort((short) value));
    }
  }

  static class VariantIntConverter extends ShreddedScalarConverter {
    VariantIntConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addInt(int value) {
      parent.build(builder -> builder.appendInt(value));
    }
  }

  static class VariantLongConverter extends ShreddedScalarConverter {
    VariantLongConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addLong(long value) {
      parent.build(builder -> builder.appendLong(value));
    }
  }

  static class VariantDateConverter extends ShreddedScalarConverter {
    VariantDateConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addInt(int value) {
      parent.build(builder -> builder.appendDate(value));
    }
  }

  static class VariantTimeConverter extends ShreddedScalarConverter {
    VariantTimeConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addLong(long value) {
      parent.build(builder -> builder.appendTime(value));
    }
  }

  static class VariantTimestampConverter extends ShreddedScalarConverter {
    VariantTimestampConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addLong(long value) {
      parent.build(builder -> builder.appendTimestampTz(value));
    }
  }

  static class VariantTimestampNtzConverter extends ShreddedScalarConverter {
    VariantTimestampNtzConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addLong(long value) {
      parent.build(builder -> builder.appendTimestampNtz(value));
    }
  }

  static class VariantTimestampNanosConverter extends ShreddedScalarConverter {
    VariantTimestampNanosConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addLong(long value) {
      parent.build(builder -> builder.appendTimestampNanosTz(value));
    }
  }

  static class VariantTimestampNanosNtzConverter extends ShreddedScalarConverter {
    VariantTimestampNanosNtzConverter(ParentConverter<VariantBuilder> parentBuilder) {
      super(parentBuilder);
    }

    @Override
    public void addLong(long value) {
      parent.build(builder -> builder.appendTimestampNanosNtz(value));
    }
  }

  /**
   * Base class for value and metadata converters, that both return a binary.
   */
  private abstract static class BinaryConverter extends PrimitiveConverter {
    Binary[] dict;

    private BinaryConverter() {
      dict = null;
    }

    protected abstract void handleBinary(Binary value);

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
    public void addBinary(Binary value) {
      handleBinary(value);
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      handleBinary(dict[dictionaryId]);
    }
  }

  private static boolean isBinary(Type field) {
    return field.isPrimitive() && field.asPrimitiveType().getPrimitiveTypeName() == BINARY;
  }

  private static boolean isShreddedArray(Type typedValueField) {
    return typedValueField.getLogicalTypeAnnotation() instanceof ListLogicalTypeAnnotation;
  }

  private static boolean isShreddedObject(Type typedValueField) {
    return !typedValueField.isPrimitive() && !isShreddedArray(typedValueField);
  }
}
