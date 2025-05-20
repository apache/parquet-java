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

class VariantConverters {
  // do not allow instantiating this class
  private VariantConverters() {}

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
  static class VariantElementConverter extends GroupConverter implements VariantConverter {

    private int valueIdx = -1;
    private int typedValueIdx = -1;
    protected Converter[] converters;
    // The binary produces by the `value` field of this converter.
    private Binary variantValue = null;

    // The following are only used if this is an object field.
    private String objectFieldName = null;
    private VariantConverter parent = null;

    // True if typed_value is an object (i.e. non-list group).
    private boolean typedValueIsObject = false;
    // Only used if typedValueIsObject is true.
    private Set<String> shreddedObjectKeys;

    @Override
    public VariantConverter getParent() {
      return this.parent;
    }

    @Override
    public VariantBuilder getBuilder() {
      return this.parent.getBuilder();
    }

    /**
     * Called by the `value` converter to provide a new value.
     */
    public void setValueBinary(Binary variantValue) {
      this.variantValue = variantValue;
    }

    public VariantElementConverter(VariantConverter parent, GroupType variantSchema, String objectFieldName) {
      this(parent, variantSchema);
      this.objectFieldName = objectFieldName;
    }

    public VariantElementConverter(VariantConverter parent, GroupType variantSchema) {
      this.parent = parent;
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
        Converter typedConverter;
        Type field = fields.get(typedValueIdx);
        LogicalTypeAnnotation annotation = field.getLogicalTypeAnnotation();
        if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
          typedConverter = new VariantArrayConverter(this, field.asGroupType());
        } else if (!field.isPrimitive()) {
          GroupType typedValue = field.asGroupType();
          typedConverter = new VariantObjectConverter(this, typedValue);
          typedValueIsObject = true;
          shreddedObjectKeys = new HashSet<>();
          for (Type f : typedValue.getFields()) {
            shreddedObjectKeys.add(f.getName());
          }
        } else {
          typedConverter = ShreddedScalarConverter.create(this, field.asPrimitiveType());
        }

        converters[typedValueIdx] = typedConverter;
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    /**
     * runtime calls
     **/
    @Override
    public void start() {
      this.variantValue = null;
      if (objectFieldName != null) {
        // Having a non-null element does not guarantee that we actually want to add this field, because
        // if value and typed_value are both null, it's a missing field. In that case, we'll detect it
        // in end() and reverse our decision to add this key.
        ((VariantObjectBuilder) getBuilder()).appendKey(objectFieldName);
      }
      // We need to remember the state of the writer so that we can tell if anything was written by typed_value.
      if (getBuilder() != null) {
        getBuilder().markCurrentWriteState();
      }
    }

    @Override
    public void end() {
      VariantBuilder builder = getBuilder();

      boolean appendedValue = false;
      if (typedValueIsObject) {
        appendedValue = endObject();
      } else {
        appendedValue = builder.hasWriteStateChanged();
        if (appendedValue && variantValue != null) {
          throw new IllegalArgumentException("Invalid variant, conflicting value and typed_value");
        } else if (variantValue != null) {
          builder.appendEncodedValue(variantValue.toByteBuffer());
          appendedValue = true;
        }
      }

      if (!appendedValue) {
        // There was no value or typed_value. If this is an object field, it is missing. It is invalid for this
        // to
        // occur for an array element or top-level value, but the spec recommends treating it as Variant Null.
        if (objectFieldName != null) {
          ((VariantObjectBuilder) builder).dropLastKey();
        } else {
          builder.appendNull();
        }
      }
    }

    /**
     * If typed_value was an object, append variantValue to the object.
     * @return true if a value was appended from either value or typed_value.
     */
    private boolean endObject() {
      VariantObjectBuilder objectBuilder = null;
      // Get the builder that the child typed_value has been adding its fields to. We need to possibly add
      // more values from the `value` field, then finalize. If the value was not an object, fields will be
      // null.
      objectBuilder = ((VariantObjectConverter) converters[typedValueIdx]).consumeObjectBuilder();

      // If objectBuilder is non-null, then we have a partially complete object ready to write.
      // Otherwise, the typed_value converter should have written something if it was non-null.
      boolean hasObject = objectBuilder != null;

      if (variantValue != null && hasObject) {
        // Both value and typed_value were non-null. This is only valid for an object.
        Variant value = new Variant(
            variantValue.toByteBuffer(), getBuilder().getMetadata().getEncodedBuffer());
        Variant.Type basicType = value.getType();
        if (basicType != Variant.Type.OBJECT) {
          throw new IllegalArgumentException("Invalid variant, conflicting value and typed_value");
        }

        for (int i = 0; i < value.numObjectElements(); i++) {
          Variant.ObjectField field = value.getFieldAtIndex(i);
          if (shreddedObjectKeys.contains(field.key)) {
            // Skip any field ID that is also in the typed schema. This check is needed because readers
            // with
            // pushdown may not look at the value column, causing inconsistent results if a writer puth
            // a
            // given key
            // only in the value column when it was present in the typed_value schema.
            // Alternatively, we could fail at this point, since the shredding is invalid according to
            // the
            // spec.
            continue;
          }
          objectBuilder.appendKey(field.key);
          objectBuilder.appendEncodedValue(field.value.getValueBuffer());
        }
        getBuilder().endObject();
      } else if (hasObject) {
        // There is only a typed_value.
        getBuilder().endObject();
      } else if (variantValue != null) {
        // There is only a value.
        getBuilder().appendEncodedValue(variantValue.toByteBuffer());
      } else {
        // There was no value or typed value. Tell the caller that we didn't add anything.
        return false;
      }
      return true;
    }
  }

  /**
   * Base class for `value` and `metadata` converters, that both return a binary.
   */
  static class BinaryConverter extends PrimitiveConverter implements VariantConverter {
    private VariantConverter parent;
    Binary[] dict;

    public BinaryConverter(VariantConverter parent) {
      dict = null;
      this.parent = parent;
    }

    @Override
    public VariantConverter getParent() {
      return parent;
    }

    @Override
    public VariantBuilder getBuilder() {
      return null;
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
  }
  /**
   * Converter for the metadata column. It sets the current metadata in the parent converter,
   * so that it can be used by the typed_value converter on the same row.
   */
  static class VariantMetadataConverter extends BinaryConverter implements VariantConverter {

    public VariantMetadataConverter(VariantColumnConverter parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      ((VariantColumnConverter) getParent()).setMetadata(value);
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      ((VariantColumnConverter) getParent()).setMetadata(dict[dictionaryId]);
    }
  }

  // Converter for the `value` field. It does not append to VariantBuilder directly: it simply holds onto
  // its value for the parent converter to append.
  static class VariantValueConverter extends BinaryConverter implements VariantConverter {
    Binary currentValue;

    public VariantValueConverter(VariantElementConverter parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      ((VariantElementConverter) getParent()).setValueBinary(value);
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      ((VariantElementConverter) getParent()).setValueBinary(dict[dictionaryId]);
    }
  }

  // Base class for converting primitive typed_value fields.
  static class ShreddedScalarConverter extends PrimitiveConverter implements VariantConverter {
    protected VariantConverter parent;

    ShreddedScalarConverter(VariantConverter parent) {
      this.parent = parent;
    }

    @Override
    public VariantConverter getParent() {
      return parent;
    }

    @Override
    public VariantBuilder getBuilder() {
      return parent.getBuilder();
    }

    // Return an appropriate converter for the given Parquet type.
    static ShreddedScalarConverter create(VariantConverter parent, PrimitiveType primitive) {
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
  }

  static class VariantStringConverter extends ShreddedScalarConverter {
    VariantStringConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      getBuilder().appendString(value.toStringUsingUTF8());
    }
  }

  static class VariantBinaryConverter extends ShreddedScalarConverter {
    VariantBinaryConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      getBuilder().appendBinary(value.toByteBuffer());
    }
  }

  static class VariantDecimalConverter extends ShreddedScalarConverter {
    private int scale;

    VariantDecimalConverter(VariantConverter parent, int scale) {
      super(parent);
      this.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      getBuilder().appendDecimal(new BigDecimal(new BigInteger(value.getBytes()), scale));
    }

    @Override
    public void addLong(long value) {
      BigDecimal decimal = BigDecimal.valueOf(value, scale);
      getBuilder().appendDecimal(decimal);
    }

    @Override
    public void addInt(int value) {
      BigDecimal decimal = BigDecimal.valueOf(value, scale);
      getBuilder().appendDecimal(decimal);
    }
  }

  static class VariantUUIDConverter extends ShreddedScalarConverter {
    VariantUUIDConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addBinary(Binary value) {
      getBuilder().appendUUIDBytes(value.toByteBuffer());
    }
  }

  static class VariantBooleanConverter extends ShreddedScalarConverter {
    VariantBooleanConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addBoolean(boolean value) {
      getBuilder().appendBoolean(value);
    }
  }

  static class VariantDoubleConverter extends ShreddedScalarConverter {
    VariantDoubleConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addDouble(double value) {
      getBuilder().appendDouble(value);
    }
  }

  static class VariantFloatConverter extends ShreddedScalarConverter {
    VariantFloatConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addFloat(float value) {
      getBuilder().appendFloat(value);
    }
  }

  static class VariantByteConverter extends ShreddedScalarConverter {
    VariantByteConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      getBuilder().appendByte((byte) value);
    }
  }

  static class VariantShortConverter extends ShreddedScalarConverter {
    VariantShortConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      getBuilder().appendShort((short) value);
    }
  }

  static class VariantIntConverter extends ShreddedScalarConverter {
    VariantIntConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      getBuilder().appendInt(value);
    }
  }

  static class VariantLongConverter extends ShreddedScalarConverter {
    VariantLongConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addLong(long value) {
      getBuilder().appendLong(value);
    }
  }

  static class VariantDateConverter extends ShreddedScalarConverter {
    VariantDateConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addInt(int value) {
      getBuilder().appendDate(value);
    }
  }

  static class VariantTimeConverter extends ShreddedScalarConverter {
    VariantTimeConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addLong(long value) {
      getBuilder().appendTime(value);
    }
  }

  static class VariantTimestampConverter extends ShreddedScalarConverter {
    VariantTimestampConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addLong(long value) {
      getBuilder().appendTimestampTz(value);
    }
  }

  static class VariantTimestampNtzConverter extends ShreddedScalarConverter {
    VariantTimestampNtzConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addLong(long value) {
      getBuilder().appendTimestampNtz(value);
    }
  }

  static class VariantTimestampNanosConverter extends ShreddedScalarConverter {
    VariantTimestampNanosConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addLong(long value) {
      getBuilder().appendTimestampNanosTz(value);
    }
  }

  static class VariantTimestampNanosNtzConverter extends ShreddedScalarConverter {
    VariantTimestampNanosNtzConverter(VariantConverter parent) {
      super(parent);
    }

    @Override
    public void addLong(long value) {
      getBuilder().appendTimestampNanosNtz(value);
    }
  }

  /**
   * Converter for a LIST typed_value.
   */
  static class VariantArrayConverter extends GroupConverter implements VariantConverter {
    private VariantConverter parent;
    private VariantArrayRepeatedConverter repeatedConverter;
    private VariantBuilder arrayBuilder;

    public VariantArrayConverter(VariantConverter parent, GroupType listType) {
      this.parent = parent;
      if (listType.getFieldCount() != 1) {
        throw new IllegalArgumentException("LIST must have one field");
      }
      Type middleLevel = listType.getType(0);
      if (!middleLevel.isRepetition(REPEATED)
          || middleLevel.isPrimitive()
          || middleLevel.asGroupType().getFieldCount() != 1) {
        throw new IllegalArgumentException("LIST must have one repeated field");
      }
      this.repeatedConverter = new VariantArrayRepeatedConverter(this, middleLevel.asGroupType(), this);
    }

    @Override
    public VariantConverter getParent() {
      return this.parent;
    }

    @Override
    public VariantBuilder getBuilder() {
      return arrayBuilder;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return repeatedConverter;
    }

    @Override
    public void start() {
      arrayBuilder = getParent().getBuilder().startArray();
    }

    @Override
    public void end() {
      getParent().getBuilder().endArray();
    }
  }

  /**
   * Converter for the repeated field of a LIST typed_value.
   */
  static class VariantArrayRepeatedConverter extends GroupConverter implements VariantConverter {
    private VariantElementConverter elementConverter;
    private VariantConverter parent;

    public VariantArrayRepeatedConverter(
        VariantConverter parent, GroupType repeatedType, VariantArrayConverter parentaConverter) {
      this.parent = parent;
      this.elementConverter =
          new VariantElementConverter(this, repeatedType.getType(0).asGroupType());
    }

    @Override
    public VariantConverter getParent() {
      return this.parent;
    }

    @Override
    public VariantBuilder getBuilder() {
      return this.parent.getBuilder();
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
    private VariantConverter parent;
    private VariantObjectBuilder objectBuilder;
    private VariantElementConverter[] converters;

    public VariantObjectConverter(VariantConverter parent, GroupType typed_value) {
      this.parent = parent;
      List<Type> fields = typed_value.getFields();
      converters = new VariantElementConverter[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        GroupType field = fields.get(i).asGroupType();
        String name = fields.get(i).getName();
        converters[i] = new VariantElementConverter(this, field, name);
      }
    }

    @Override
    public VariantConverter getParent() {
      return this.parent;
    }

    @Override
    public VariantBuilder getBuilder() {
      return objectBuilder;
    }

    /**
     * This method must be called after each call to end() to ensure that the object builder is
     * not reused on a subsequent value for which start() was never called.
     *
     * @return The partially built object builder, or null if no object was constructed.
     */
    VariantObjectBuilder consumeObjectBuilder() {
      VariantObjectBuilder objectBuilder = this.objectBuilder;
      this.objectBuilder = null;
      return objectBuilder;
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      objectBuilder = getParent().getBuilder().startObject();
    }

    @Override
    public void end() {
      // We can't finish writing the object here, because there might be residual entries in our
      // parent's value column. The parent converter calls consumeObjectBuilder to finalize the object.
    }
  }
}
