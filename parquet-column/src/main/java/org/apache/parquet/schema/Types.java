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
package org.apache.parquet.schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.parquet.Preconditions;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.ID;

/**
 * This class provides fluent builders that produce Parquet schema Types.
 * <p>
 * The most basic use is to build primitive types:
 * <pre>
 *   Types.required(INT64).named("id");
 *   Types.optional(INT32).named("number");
 * </pre>
 * <p>
 * The {@link #required(PrimitiveTypeName)} factory method produces a primitive
 * type builder, and the {@link PrimitiveBuilder#named(String)} builds the
 * {@link PrimitiveType}. Between {@code required} and {@code named}, other
 * builder methods can be used to add type annotations or other type metadata:
 * <pre>
 *   Types.required(BINARY).as(UTF8).named("username");
 *   Types.optional(FIXED_LEN_BYTE_ARRAY).length(20).named("sha1");
 * </pre>
 * <p>
 * Optional types are built using {@link #optional(PrimitiveTypeName)} to get
 * the builder.
 * <p>
 * Groups are built similarly, using {@code requiredGroup()} (or the optional
 * version) to return a group builder. Group builders provide {@code required}
 * and {@code optional} to add primitive types, which return primitive builders
 * like the versions above.
 * <pre>
 *   // This produces:
 *   // required group User {
 *   //   required int64 id;
 *   //   optional binary email (UTF8);
 *   // }
 *   Types.requiredGroup()
 *            .required(INT64).named("id")
 *            .required(BINARY).as(UTF8).named("email")
 *        .named("User")
 * </pre>
 * <p>
 * When {@code required} is called on a group builder, the builder it returns
 * will add the type to the parent group when it is built and {@code named} will
 * return its parent group builder (instead of the type) so more fields can be
 * added.
 * <p>
 * Sub-groups can be created using {@code requiredGroup()} to get a group
 * builder that will create the group type, add it to the parent builder, and
 * return the parent builder for more fields.
 * <pre>
 *   // required group User {
 *   //   required int64 id;
 *   //   optional binary email (UTF8);
 *   //   optional group address {
 *   //     required binary street (UTF8);
 *   //     required int32 zipcode;
 *   //   }
 *   // }
 *   Types.requiredGroup()
 *            .required(INT64).named("id")
 *            .required(BINARY).as(UTF8).named("email")
 *            .optionalGroup()
 *                .required(BINARY).as(UTF8).named("street")
 *                .required(INT32).named("zipcode")
 *            .named("address")
 *        .named("User")
 * </pre>
 * <p>
 * Maps are built similarly, using {@code requiredMap()} (or the optionalMap()
 * version) to return a map builder. Map builders provide {@code key} to add
 * a primitive as key or a {@code groupKey} to add a group as key. {@code key()}
 * returns a MapKey builder, which extends a primitive builder. On the other hand,
 * {@code groupKey()} returns a MapGroupKey builder, which extends a group builder.
 * A key in a map is always required.
 * <p>
 * Once a key is built, a primitive map value can be built using {@code requiredValue()}
 * (or the optionalValue() version) that returns MapValue builder. A group map value
 * can be built using {@code requiredGroupValue()} (or the optionalGroupValue()
 * version) that returns MapGroupValue builder.
 *
 *   // required group zipMap (MAP) {
 *   //   repeated group map (MAP_KEY_VALUE) {
 *   //     required float key
 *   //     optional int32 value
 *   //   }
 *   // }
 *   Types.requiredMap()
 *            .key(FLOAT)
 *            .optionalValue(INT32)
 *        .named("zipMap")
 *
 *
 *   // required group zipMap (MAP) {
 *   //   repeated group map (MAP_KEY_VALUE) {
 *   //     required group key {
 *   //       optional int64 first;
 *   //       required group second {
 *   //         required float inner_id_1;
 *   //         optional int32 inner_id_2;
 *   //       }
 *   //     }
 *   //     optional group value {
 *   //       optional group localGeoInfo {
 *   //         required float inner_value_1;
 *   //         optional int32 inner_value_2;
 *   //       }
 *   //       optional int32 zipcode;
 *   //     }
 *   //   }
 *   // }
 *   Types.requiredMap()
 *            .groupKey()
 *              .optional(INT64).named("id")
 *              .requiredGroup()
 *                .required(FLOAT).named("inner_id_1")
 *                .required(FLOAT).named("inner_id_2")
 *              .named("second")
 *            .optionalGroup()
 *              .optionalGroup()
 *                .required(FLOAT).named("inner_value_1")
 *                .optional(INT32).named("inner_value_2")
 *              .named("localGeoInfo")
 *              .optional(INT32).named("zipcode")
 *        .named("zipMap")
 * </pre>
 * <p>
 * Message types are built using {@link #buildMessage()} and function just like
 * group builders.
 * <pre>
 *   // message User {
 *   //   required int64 id;
 *   //   optional binary email (UTF8);
 *   //   optional group address {
 *   //     required binary street (UTF8);
 *   //     required int32 zipcode;
 *   //   }
 *   // }
 *   Types.buildMessage()
 *            .required(INT64).named("id")
 *            .required(BINARY).as(UTF8).named("email")
 *            .optionalGroup()
 *                .required(BINARY).as(UTF8).named("street")
 *                .required(INT32).named("zipcode")
 *            .named("address")
 *        .named("User")
 * </pre>
 * <p>
 * These builders enforce consistency checks based on the specifications in
 * the parquet-format documentation. For example, if DECIMAL is used to annotate
 * a FIXED_LEN_BYTE_ARRAY that is not long enough for its maximum precision,
 * these builders will throw an IllegalArgumentException:
 * <pre>
 *   // throws IllegalArgumentException with message:
 *   // "FIXED(4) is not long enough to store 10 digits"
 *   Types.required(FIXED_LEN_BYTE_ARRAY).length(4)
 *        .as(DECIMAL).precision(10)
 *        .named("badDecimal");
 * </pre>
 */
public class Types {
  private static final int NOT_SET = 0;

  /**
   * A base builder for {@link Type} objects.
   *
   * @param <P> The type that this builder will return from
   *          {@link #named(String)} when the type is built.
   */
  public abstract static class Builder<T extends Builder, P> {
    protected final P parent;
    protected final Class<? extends P> returnClass;

    protected Type.Repetition repetition = null;
    protected OriginalType originalType = null;
    protected Type.ID id = null;
    private boolean repetitionAlreadySet = false;

    /**
     * Construct a type builder that returns a "parent" object when the builder
     * is finished. The {@code parent} will be returned by
     * {@link #named(String)} so that builders can be chained.
     *
     * @param parent a non-null object to return from {@link #named(String)}
     */
    protected Builder(P parent) {
      Preconditions.checkNotNull(parent, "Parent cannot be null");
      this.parent = parent;
      this.returnClass = null;
    }

    /**
     * Construct a type builder that returns the {@link Type} that was built
     * when the builder is finished. The {@code returnClass} must be the
     * expected {@code Type} class.
     *
     * @param returnClass a {@code Type} to return from {@link #named(String)}
     */
    protected Builder(Class<P> returnClass) {
      Preconditions.checkArgument(Type.class.isAssignableFrom(returnClass),
          "The requested return class must extend Type");
      this.returnClass = returnClass;
      this.parent = null;
    }

    protected abstract T self();

    protected final T repetition(Type.Repetition repetition) {
      Preconditions.checkArgument(!repetitionAlreadySet,
          "Repetition has already been set");
      Preconditions.checkNotNull(repetition, "Repetition cannot be null");
      this.repetition = repetition;
      this.repetitionAlreadySet = true;
      return self();
    }

    /**
     * Adds a type annotation ({@link OriginalType}) to the type being built.
     * <p>
     * Type annotations are used to extend the types that parquet can store, by
     * specifying how the primitive types should be interpreted. This keeps the
     * set of primitive types to a minimum and reuses parquet's efficient
     * encodings. For example, strings are stored as byte arrays (binary) with
     * a UTF8 annotation.
     *
     * @param type an {@code OriginalType}
     * @return this builder for method chaining
     */
    public T as(OriginalType type) {
      this.originalType = type;
      return self();
    }

    /**
     * adds an id annotation to the type being built.
     * <p>
     * ids are used to capture the original id when converting from models using ids (thrift, protobufs)
     *
     * @param id the id of the field
     * @return this builder for method chaining
     */
    public T id(int id) {
      this.id = new ID(id);
      return self();
    }

    abstract protected Type build(String name);

    /**
     * Builds a {@link Type} and returns the parent builder, if given, or the
     * {@code Type} that was built. If returning a parent object that is a
     * GroupBuilder, the constructed type will be added to it as a field.
     * <p>
     * <em>Note:</em> Any configuration for this type builder should be done
     * before calling this method.
     *
     * @param name a name for the constructed type
     * @return the parent {@code GroupBuilder} or the constructed {@code Type}
     */
    public P named(String name) {
      Preconditions.checkNotNull(name, "Name is required");
      Preconditions.checkNotNull(repetition, "Repetition is required");

      Type type = build(name);
      if (parent != null) {
        // if the parent is a BaseGroupBuilder, add type to it
        if (BaseGroupBuilder.class.isAssignableFrom(parent.getClass())) {
          BaseGroupBuilder.class.cast(parent).addField(type);
        } else if (parent instanceof BaseListBuilder) {
          ((BaseListBuilder) parent).elementType = type;
        }
        return parent;
      } else {
        // no parent indicates that the Type object should be returned
        // the constructor check guarantees that returnClass is a Type
        return returnClass.cast(type);
      }
    }

  }

  public abstract static class BasePrimitiveBuilder<P,
      Q extends BasePrimitiveBuilder<P, Q>> extends Builder<Q, P> {
    private static final long MAX_PRECISION_INT32 = maxPrecision(4);
    private static final long MAX_PRECISION_INT64 = maxPrecision(8);
    private final PrimitiveTypeName primitiveType;
    private int length = NOT_SET;
    private int precision = NOT_SET;
    private int scale = NOT_SET;

    private BasePrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent);
      this.primitiveType = type;
    }

    private BasePrimitiveBuilder(Class<P> returnType, PrimitiveTypeName type) {
      super(returnType);
      this.primitiveType = type;
    }

    @Override
    protected abstract Q self();

    /**
     * Adds the length for a FIXED_LEN_BYTE_ARRAY.
     *
     * @param length an int length
     * @return this builder for method chaining
     */
    public BasePrimitiveBuilder<P, Q> length(int length) {
      this.length = length;
      return this;
    }

    /**
     * Adds the precision for a DECIMAL.
     * <p>
     * This value is required for decimals and must be less than or equal to
     * the maximum number of base-10 digits in the underlying type. A 4-byte
     * fixed, for example, can store up to 9 base-10 digits.
     *
     * @param precision an int precision value for the DECIMAL
     * @return this builder for method chaining
     */
    public BasePrimitiveBuilder<P, Q> precision(int precision) {
      this.precision = precision;
      return this;
    }

    /**
     * Adds the scale for a DECIMAL.
     * <p>
     * This value must be less than the maximum precision of the type and must
     * be a positive number. If not set, the default scale is 0.
     * <p>
     * The scale specifies the number of digits of the underlying unscaled
     * that are to the right of the decimal point. The decimal interpretation
     * of values in this column is: {@code value*10^(-scale)}.
     *
     * @param scale an int scale value for the DECIMAL
     * @return this builder for method chaining
     */
    public BasePrimitiveBuilder<P, Q> scale(int scale) {
      this.scale = scale;
      return this;
    }

    @Override
    protected PrimitiveType build(String name) {
      if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == primitiveType) {
        Preconditions.checkArgument(length > 0,
            "Invalid FIXED_LEN_BYTE_ARRAY length: " + length);
      }

      DecimalMetadata meta = decimalMetadata();

      // validate type annotations and required metadata
      if (originalType != null) {
        switch (originalType) {
          case UTF8:
          case JSON:
          case BSON:
            Preconditions.checkState(
                primitiveType == PrimitiveTypeName.BINARY,
                originalType.toString() + " can only annotate binary fields");
            break;
          case DECIMAL:
            Preconditions.checkState(
                (primitiveType == PrimitiveTypeName.INT32) ||
                (primitiveType == PrimitiveTypeName.INT64) ||
                (primitiveType == PrimitiveTypeName.BINARY) ||
                (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY),
                "DECIMAL can only annotate INT32, INT64, BINARY, and FIXED"
            );
            if (primitiveType == PrimitiveTypeName.INT32) {
              Preconditions.checkState(
                  meta.getPrecision() <= MAX_PRECISION_INT32,
                  "INT32 cannot store " + meta.getPrecision() + " digits " +
                      "(max " + MAX_PRECISION_INT32 + ")");
            } else if (primitiveType == PrimitiveTypeName.INT64) {
              Preconditions.checkState(
                  meta.getPrecision() <= MAX_PRECISION_INT64,
                  "INT64 cannot store " + meta.getPrecision() + " digits " +
                  "(max " + MAX_PRECISION_INT64 + ")");
            } else if (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
              Preconditions.checkState(
                  meta.getPrecision() <= maxPrecision(length),
                  "FIXED(" + length + ") cannot store " + meta.getPrecision() +
                  " digits (max " + maxPrecision(length) + ")");
            }
            break;
          case DATE:
          case TIME_MILLIS:
          case UINT_8:
          case UINT_16:
          case UINT_32:
          case INT_8:
          case INT_16:
          case INT_32:
            Preconditions.checkState(primitiveType == PrimitiveTypeName.INT32,
                originalType.toString() + " can only annotate INT32");
            break;
          case TIMESTAMP_MILLIS:
          case UINT_64:
          case INT_64:
            Preconditions.checkState(primitiveType == PrimitiveTypeName.INT64,
                originalType.toString() + " can only annotate INT64");
            break;
          case INTERVAL:
            Preconditions.checkState(
                (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) &&
                (length == 12),
                "INTERVAL can only annotate FIXED_LEN_BYTE_ARRAY(12)");
            break;
          case ENUM:
            Preconditions.checkState(
                primitiveType == PrimitiveTypeName.BINARY,
                "ENUM can only annotate binary fields");
            break;
          default:
            throw new IllegalStateException(originalType + " can not be applied to a primitive type");
        }
      }

      return new PrimitiveType(repetition, primitiveType, length, name, originalType, meta, id);
    }

    private static long maxPrecision(int numBytes) {
      return Math.round(                      // convert double to long
          Math.floor(Math.log10(              // number of base-10 digits
              Math.pow(2, 8 * numBytes - 1) - 1)  // max value stored in numBytes
          )
      );
    }

    protected DecimalMetadata decimalMetadata() {
      DecimalMetadata meta = null;
      if (OriginalType.DECIMAL == originalType) {
        Preconditions.checkArgument(precision > 0,
            "Invalid DECIMAL precision: " + precision);
        Preconditions.checkArgument(scale >= 0,
            "Invalid DECIMAL scale: " + scale);
        Preconditions.checkArgument(scale <= precision,
            "Invalid DECIMAL scale: cannot be greater than precision");
        meta = new DecimalMetadata(precision, scale);
      }
      return meta;
    }
  }

  /**
   * A builder for {@link PrimitiveType} objects.
   *
   * @param <P> The type that this builder will return from
   *          {@link #named(String)} when the type is built.
   */
  public static class PrimitiveBuilder<P> extends BasePrimitiveBuilder<P, PrimitiveBuilder<P>> {

    private PrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent, type);
    }

    private PrimitiveBuilder(Class<P> returnType, PrimitiveTypeName type) {
      super(returnType, type);
    }

    @Override
    protected PrimitiveBuilder<P> self() {
      return this;
    }
  }

  public abstract static class BaseGroupBuilder<P, Q extends BaseGroupBuilder<P, Q>> extends
      Builder<Q, P> {
    protected final List<Type> fields;

    private BaseGroupBuilder(P parent) {
      super(parent);
      this.fields = new ArrayList<Type>();
    }

    private BaseGroupBuilder(Class<P> returnType) {
      super(returnType);
      this.fields = new ArrayList<Type>();
    }

    @Override
    protected abstract Q self();

    public PrimitiveBuilder<Q> primitive(
        PrimitiveTypeName type, Type.Repetition repetition) {
      return new PrimitiveBuilder<Q> (self(), type)
          .repetition(repetition);
    }

    /**
     * Returns a {@link PrimitiveBuilder} for the required primitive type
     * {@code type}.
     *
     * @param type a {@link PrimitiveTypeName}
     * @return a primitive builder for {@code type} that will return this
     *          builder for additional fields.
     */
    public PrimitiveBuilder<Q> required(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<Q>(self(), type)
          .repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Returns a {@link PrimitiveBuilder} for the optional primitive type
     * {@code type}.
     *
     * @param type a {@link PrimitiveTypeName}
     * @return a primitive builder for {@code type} that will return this
     *          builder for additional fields.
     */
    public PrimitiveBuilder<Q> optional(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<Q>(self(), type)
          .repetition(Type.Repetition.OPTIONAL);
    }

    /**
     * Returns a {@link PrimitiveBuilder} for the repeated primitive type
     * {@code type}.
     *
     * @param type a {@link PrimitiveTypeName}
     * @return a primitive builder for {@code type} that will return this
     *          builder for additional fields.
     */
    public PrimitiveBuilder<Q> repeated(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<Q>(self(), type)
          .repetition(Type.Repetition.REPEATED);
    }

    public GroupBuilder<Q> group(Type.Repetition repetition) {
      return new GroupBuilder<Q>(self())
          .repetition(repetition);
    }

    /**
     * Returns a {@link GroupBuilder} to build a required sub-group.
     *
     * @return a group builder that will return this builder for additional
     *          fields.
     */
    public GroupBuilder<Q> requiredGroup() {
      return new GroupBuilder<Q>(self())
          .repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Returns a {@link GroupBuilder} to build an optional sub-group.
     *
     * @return a group builder that will return this builder for additional
     *          fields.
     */
    public GroupBuilder<Q> optionalGroup() {
      return new GroupBuilder<Q>(self())
          .repetition(Type.Repetition.OPTIONAL);
    }

    /**
     * Returns a {@link GroupBuilder} to build a repeated sub-group.
     *
     * @return a group builder that will return this builder for additional
     *          fields.
     */
    public GroupBuilder<Q> repeatedGroup() {
      return new GroupBuilder<Q>(self())
          .repetition(Type.Repetition.REPEATED);
    }

    /**
     * Adds {@code type} as a sub-field to the group configured by this builder.
     *
     * @return this builder for additional fields.
     */
    public Q addField(Type type) {
      fields.add(type);
      return self();
    }

    /**
     * Adds {@code types} as sub-fields of the group configured by this builder.
     *
     * @return this builder for additional fields.
     */
    public Q addFields(Type... types) {
      Collections.addAll(fields, types);
      return self();
    }

    @Override
    protected GroupType build(String name) {
      Preconditions.checkState(!fields.isEmpty(),
          "Cannot build an empty group");
      return new GroupType(repetition, name, originalType, fields, id);
    }

    public MapBuilder<Q> map(
        Type.Repetition repetition) {
      return new MapBuilder<Q>(self()).repetition(repetition);
    }

    public MapBuilder<Q> requiredMap() {
      return new MapBuilder<Q>(self())
          .repetition(Type.Repetition.REQUIRED);
    }

    public MapBuilder<Q> optionalMap() {
      return new MapBuilder<Q>(self())
          .repetition(Type.Repetition.OPTIONAL);
    }

    public ListBuilder<BaseGroupBuilder<P, Q>> list(Type.Repetition repetition) {
      return new ListBuilder<BaseGroupBuilder<P, Q>>(this).repetition(repetition);
    }

    public ListBuilder<BaseGroupBuilder<P, Q>> requiredList() {
      return list(Type.Repetition.REQUIRED);
    }

    public ListBuilder<BaseGroupBuilder<P, Q>> optionalList() {
      return list(Type.Repetition.OPTIONAL);
    }
  }

  /**
   * A builder for {@link GroupType} objects.
   *
   * @param <P> The type that this builder will return from
   *          {@link #named(String)} when the type is built.
   */
  public static class GroupBuilder<P> extends BaseGroupBuilder<P, GroupBuilder<P>> {

    private GroupBuilder(P parent) {
      super(parent);
    }

    private GroupBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected GroupBuilder<P> self() {
      return this;
    }
  }

  public abstract static class BaseMapBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
      Builder<Q, P> {
    private static final Type STRING_KEY = Types
        .required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("key");

    public static class MapKeyBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BasePrimitiveBuilder<P, MapKeyBuilder<P, Q>> {
      private BaseMapBuilder<P, Q> parent = null;

      public MapKeyBuilder(BaseMapBuilder<P, Q> parent, PrimitiveTypeName type) {
        super(parent.parent, type);
        this.parent = parent;
        repetition(Type.Repetition.REQUIRED);
      }

      public MapKeyBuilder(BaseMapBuilder<P, Q> parent, Class<P> returnType,
                           PrimitiveTypeName type) {
        super(returnType, type);
        this.parent = parent;
        repetition(Type.Repetition.REQUIRED);
      }

      public MapValueBuilder<P, Q> value(PrimitiveTypeName type, Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        if (parent.parent != null) {
          return new MapValueBuilder<P, Q>(parent,
              type).repetition(repetition);
        } else {
          return new MapValueBuilder<P, Q>(parent,
              parent.returnType, type).repetition(repetition);
        }
      }

      public MapValueBuilder<P, Q> requiredValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.REQUIRED);
      }

      public MapValueBuilder<P, Q> optionalValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.OPTIONAL);
      }

      public MapGroupValueBuilder<P, Q> groupValue(Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        return new MapGroupValueBuilder<P, Q>(parent).repetition(repetition);
      }

      public MapGroupValueBuilder<P, Q> requiredGroupValue() {
        return groupValue(Type.Repetition.REQUIRED);
      }

      public MapGroupValueBuilder<P, Q> optionalGroupValue() {
        return groupValue(Type.Repetition.OPTIONAL);
      }

      public MapMapValueBuilder<P, Q> mapValue(Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        return new MapMapValueBuilder<P, Q>(parent).repetition(repetition);
      }

      public MapMapValueBuilder<P, Q> requiredMapValue() {
        return mapValue(Type.Repetition.REQUIRED);
      }

      public MapMapValueBuilder<P, Q> optionalMapValue() {
        return mapValue(Type.Repetition.OPTIONAL);
      }

      public MapListValueBuilder<P, Q> listValue(Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        return new MapListValueBuilder<P, Q>(parent).repetition(repetition);
      }

      public MapListValueBuilder<P, Q> requiredListValue() {
        return listValue(Type.Repetition.REQUIRED);
      }

      public MapListValueBuilder<P, Q> optionalListValue() {
        return listValue(Type.Repetition.OPTIONAL);
      }

      @Override
      public P named(String name) {
        parent.setKeyType(build("key"));
        return parent.named(name);
      }

      @Override
      protected MapKeyBuilder<P, Q> self() {
        return this;
      }
    }

    public static class MapValueBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BasePrimitiveBuilder<P, MapValueBuilder<P, Q>> {
      private final BaseMapBuilder<P, Q> parent;

      public MapValueBuilder(BaseMapBuilder<P, Q> parent, PrimitiveTypeName type) {
        super(parent.parent, type);
        this.parent = parent;
      }

      public MapValueBuilder(BaseMapBuilder<P, Q> parent, Class<P> returnType,
                             PrimitiveTypeName type) {
        super(returnType, type);
        this.parent = parent;
      }

      public P named(String name) {
        parent.setValueType(build("value"));
        return parent.named(name);
      }

      @Override
      protected MapValueBuilder<P, Q> self() {
        return this;
      }
    }

    public static class MapGroupKeyBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BaseGroupBuilder<P, MapGroupKeyBuilder<P, Q>> {
      private final BaseMapBuilder<P, Q> parent;

      public MapGroupKeyBuilder(BaseMapBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
        repetition(Type.Repetition.REQUIRED);
      }

      @Override
      protected MapGroupKeyBuilder<P, Q> self() {
        return this;
      }

      public MapValueBuilder<P, Q> value(PrimitiveTypeName type, Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        if (parent.parent != null) {
          return new MapValueBuilder<P, Q>(parent,
              type).repetition(repetition);
        } else {
          return new MapValueBuilder<P, Q>(parent,
              parent.returnType, type).repetition(repetition);
        }
      }

      public MapValueBuilder<P, Q> requiredValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.REQUIRED);
      }

      public MapValueBuilder<P, Q> optionalValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.OPTIONAL);
      }

      public MapGroupValueBuilder<P, Q> groupValue(Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        return new MapGroupValueBuilder<P, Q>(parent).repetition(repetition);
      }

      public MapGroupValueBuilder<P, Q> requiredGroupValue() {
        return groupValue(Type.Repetition.REQUIRED);
      }

      public MapGroupValueBuilder<P, Q> optionalGroupValue() {
        return groupValue(Type.Repetition.OPTIONAL);
      }

      public MapMapValueBuilder<P, Q> mapValue(Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        return new MapMapValueBuilder<P, Q>(parent).repetition(repetition);
      }

      public MapMapValueBuilder<P, Q> requiredMapValue() {
        return mapValue(Type.Repetition.REQUIRED);
      }

      public MapMapValueBuilder<P, Q> optionalMapValue() {
        return mapValue(Type.Repetition.OPTIONAL);
      }

      public MapListValueBuilder<P, Q> listValue(Type.Repetition repetition) {
        parent.setKeyType(build("key"));
        return new MapListValueBuilder<P, Q>(parent).repetition(repetition);
      }

      public MapListValueBuilder<P, Q> requiredListValue() {
        return listValue(Type.Repetition.REQUIRED);
      }

      public MapListValueBuilder<P, Q> optionalListValue() {
        return listValue(Type.Repetition.OPTIONAL);
      }
    }

    public static class MapGroupValueBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BaseGroupBuilder<P, MapGroupValueBuilder<P, Q>> {
      private final BaseMapBuilder<P, Q> parent;

      public MapGroupValueBuilder(BaseMapBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public P named(String name) {
        parent.setValueType(build("value"));
        return parent.named(name);
      }

      @Override
      protected MapGroupValueBuilder<P, Q> self() {
        return this;
      }
    }

    public static class MapMapValueBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BaseMapBuilder<P, MapMapValueBuilder<P, Q>> {
      private final BaseMapBuilder<P, Q> parent;

      public MapMapValueBuilder(BaseMapBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public P named(String name) {
        parent.setValueType(build("value"));
        return parent.named(name);
      }

      @Override
      protected MapMapValueBuilder<P, Q> self() {
        return this;
      }
    }

    public static class MapListValueBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BaseListBuilder<P, MapListValueBuilder<P, Q>> {
      private final BaseMapBuilder<P, Q> parent;

      public MapListValueBuilder(BaseMapBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public P named(String name) {
        parent.setValueType(build("value"));
        return parent.named(name);
      }

      @Override
      protected MapListValueBuilder<P, Q> self() {
        return this;
      }
    }

    public static class MapListKeyBuilder<P, Q extends BaseMapBuilder<P, Q>> extends
        BaseListBuilder<P, MapListKeyBuilder<P, Q>> {
      private final BaseMapBuilder<P, Q> parent;

      public MapListKeyBuilder(BaseMapBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public P named(String name) {
        return super.named(name);
      }

      @Override
      protected MapListKeyBuilder<P, Q> self() {
        return this;
      }

      public void setKey(Type key) {
        parent.setKeyType(key);
      }
    }

    protected void setKeyType(Type keyType) {
      this.keyType = keyType;
    }

    protected void setValueType(Type valueType) {
      this.valueType = valueType;
    }

    private Type keyType = null;
    private Type valueType = null;
    private Class<P> returnType = null;

    public BaseMapBuilder(P parent) {
      super(parent);
    }

    private BaseMapBuilder(Class<P> returnType) {
      super(returnType);
      this.returnType = returnType;
    }

    @Override
    protected abstract Q self();

    public MapKeyBuilder<P, Q> key(PrimitiveTypeName type) {
      if (parent != null) {
        return new MapKeyBuilder<P, Q>(this, type);
      } else {
        return new MapKeyBuilder<P, Q>(this, returnType, type);
      }
    }

    public MapGroupKeyBuilder<P, Q> groupKey() {
      return new MapGroupKeyBuilder<P, Q>(this);
    }

    public MapListKeyBuilder<P, Q> listKey() {
      return new MapListKeyBuilder<P, Q>(this);
    }

    @Override
    protected Type build(String name) {
      if (keyType == null) {
        keyType = STRING_KEY;
      }
      if (valueType != null) {
        return buildGroup(repetition)
            .as(OriginalType.MAP)
            .repeatedGroup()
            .addFields(keyType, valueType)
            .named("map")
            .named(name);
      } else {
        return buildGroup(repetition)
            .as(OriginalType.MAP)
            .repeatedGroup()
            .addFields(keyType)
            .named("map")
            .named(name);
      }
    }

    public P named(String name) {
      Preconditions.checkState(originalType == null,
          "MAP is already a logical type and can't be changed.");
      return super.named(name);
    }
  }

  public static class MapBuilder<P> extends BaseMapBuilder<P, MapBuilder<P>> {
    public MapBuilder(P parent) {
      super(parent);
    }

    private MapBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected MapBuilder<P> self() {
      return this;
    }
  }

  public abstract static class BaseListBuilder<P, Q extends BaseListBuilder<P, Q>> extends
      Builder<Q, P> {
    private Type elementType = null;
    private P parent;
    private Class<P> returnType;

    public BaseListBuilder(P parent) {
      super(parent);
      this.parent = parent;
    }

    public BaseListBuilder(Class<P> returnType) {
      super(returnType);
      this.returnType = returnType;
    }

    public void setElementType(Type elementType) {
      this.elementType = elementType;
    }

    public static class ListElementBuilder<P, Q extends BaseListBuilder<P, Q>> extends
        BasePrimitiveBuilder<P,
        ListElementBuilder<P, Q>> {
      private final BaseListBuilder<P, Q> parent;

      public ListElementBuilder(BaseListBuilder<P, Q> parent, PrimitiveTypeName type) {
        super(parent.parent, type);
        this.parent = parent;
      }

      public ListElementBuilder(BaseListBuilder<P, Q> parent, Class<P> returnType, PrimitiveTypeName type) {
        super(returnType, type);
        this.parent = parent;
      }

      public P named(String name) {
        parent.setElementType(build("element"));
        return parent.named(name);
      }

      @Override
      protected ListElementBuilder<P, Q> self() {
        return this;
      }
    }

    public static class ListGroupElementBuilder<P, Q extends BaseListBuilder<P, Q>> extends
        BaseGroupBuilder<P, ListGroupElementBuilder<P, Q>> {
      private final BaseListBuilder<P, Q> parent;

      public ListGroupElementBuilder(BaseListBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public ListGroupElementBuilder(BaseListBuilder<P, Q> parent, Class<P> returnType) {
        super(returnType);
        this.parent = parent;
      }

      public P named(String name) {
        parent.setElementType(build("element"));
        return parent.named(name);
      }

      @Override
      protected ListGroupElementBuilder<P, Q> self() {
        return this;
      }
    }

    public static class ListMapElementBuilder<P, Q extends BaseListBuilder<P, Q>> extends BaseMapBuilder<P,
        ListMapElementBuilder<P, Q>> {

      BaseListBuilder<P, Q> parent;

      public ListMapElementBuilder(BaseListBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public ListMapElementBuilder(BaseListBuilder<P, Q> parent, Class<P> returnType) {
        super(returnType);
        this.parent = parent;
      }

      @Override
      protected ListMapElementBuilder<P, Q> self() {
        return this;
      }

      @Override
      protected Type build(String name) {
        parent.setElementType(super.build("element"));
        return parent.build(name);
      }
    }

    public static class ListListElementBuilder<P, Q extends BaseListBuilder<P, Q>> extends
        BaseListBuilder<P, ListListElementBuilder<P, Q>> {

      BaseListBuilder<P, Q> parent;

      public ListListElementBuilder(BaseListBuilder<P, Q> parent) {
        super(parent.parent);
        this.parent = parent;
      }

      public ListListElementBuilder(BaseListBuilder<P, Q> parent, Class<P> returnType) {
        super(returnType);
        this.parent = parent;
      }

      @Override
      protected ListListElementBuilder<P, Q> self() {
        return this;
      }

      @Override
      protected Type build(String name) {
        parent.setElementType(super.build("element"));
        return parent.build(name);
      }
    }

    public static class ListKeyElementBuilder<P, Q extends BaseListBuilder<P, Q>> extends
        BasePrimitiveBuilder<P, ListKeyElementBuilder<P, Q>> {
      private final BaseListBuilder<P, Q> parent; // this will be MapListKeyBuilder

      public ListKeyElementBuilder(BaseListBuilder<P, Q> parent, PrimitiveTypeName type) {
        super(((MapBuilder<P>) ((BaseMapBuilder.MapListKeyBuilder)parent).parent).parent,
            type);
        this.parent = parent;
      }

      public ListKeyElementBuilder(BaseListBuilder<P, Q> parent, Class<P> returnType, PrimitiveTypeName type) {
        super(returnType, type);
        this.parent = parent;
      }

      @Override
      protected ListKeyElementBuilder<P, Q> self() {
        return this;
      }

      public class ModPrimitiveBuilder<I> extends BasePrimitiveBuilder<I,
          ModPrimitiveBuilder<I>> {

        MapBuilder<I> parent;

        public ModPrimitiveBuilder(MapBuilder<I> parent, PrimitiveTypeName type) {
          super(parent.parent, type);
          this.parent = parent;
        }

        @Override
        protected ModPrimitiveBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModPrimitiveBuilder<P> value(PrimitiveTypeName type, Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModPrimitiveBuilder<P>(
            (MapBuilder<P>)(((BaseMapBuilder.MapListKeyBuilder) parent).parent),
            type).repetition(repetition);
      }

      public ModPrimitiveBuilder<P> requiredValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.REQUIRED);
      }

      public ModPrimitiveBuilder<P> optionalValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.OPTIONAL);
      }

      public class ModGroupBuilder<I> extends BaseGroupBuilder<I,
          ModGroupBuilder<I>> {

        MapBuilder<I> parent;

        public ModGroupBuilder(MapBuilder<I> parent) {
          super(parent.parent);
          this.parent = parent;
        }

        @Override
        protected ModGroupBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModGroupBuilder<P> groupValue(Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModGroupBuilder<P>(
            (MapBuilder<P>)((BaseMapBuilder.MapListKeyBuilder) parent).parent)
            .repetition(repetition);
      }

      public ModGroupBuilder<P> requiredGroupValue() {
        return groupValue(Type.Repetition.REQUIRED);
      }

      public ModGroupBuilder<P> optionalGroupValue() {
        return groupValue(Type.Repetition.OPTIONAL);
      }

      public class ModMapBuilder<I> extends BaseMapBuilder<I,
          ModMapBuilder<I>> {

        MapBuilder<I> parent;

        public ModMapBuilder(MapBuilder<I> parent) {
          super(parent.parent);
          this.parent = parent;
        }

        @Override
        protected ModMapBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModMapBuilder<P> mapValue(Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModMapBuilder<P>((MapBuilder<P>)(
            (BaseMapBuilder.MapListKeyBuilder) parent).parent).repetition(repetition);
      }

      public ModMapBuilder<P> requiredMapValue() {
        return mapValue(Type.Repetition.REQUIRED);
      }

      public ModMapBuilder<P> optionalMapValue() {
        return mapValue(Type.Repetition.OPTIONAL);
      }

      public class ModListBuilder<I> extends BaseListBuilder<I,
          ModListBuilder<I>> {

        MapBuilder<I> parent;

        public ModListBuilder(MapBuilder<I> parent) {
          super(parent.parent);
          this.parent = parent;
        }

        @Override
        protected ModListBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModListBuilder<P> listValue(Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModListBuilder<P>((MapBuilder<P>)(
            (BaseMapBuilder.MapListKeyBuilder) parent).parent).repetition(repetition);
      }

      public ModListBuilder<P> requiredListValue() {
        return listValue(Type.Repetition.REQUIRED);
      }

      public ModListBuilder<P> optionalListValue() {
        return listValue(Type.Repetition.OPTIONAL);
      }
    }

    public static class ListKeyGroupElementBuilder<P, Q extends BaseListBuilder<P, Q>> extends
        BaseGroupBuilder<P, ListKeyGroupElementBuilder<P, Q>> {
      private final BaseListBuilder<P, Q> parent; // this will be MapListKeyBuilder

      public
      ListKeyGroupElementBuilder(BaseListBuilder<P, Q> parent) {
        super(((MapBuilder<P>)((BaseMapBuilder.MapListKeyBuilder)parent).parent).parent);
        this.parent = parent;
      }

      public ListKeyGroupElementBuilder(BaseListBuilder<P, Q> parent, Class<P> returnType) {
        super(returnType);
        this.parent = parent;
      }

      @Override
      protected ListKeyGroupElementBuilder<P, Q> self() {
        return this;
      }

      public class ModPrimitiveBuilder<I> extends BasePrimitiveBuilder<I,
          ModPrimitiveBuilder<I>> {

        MapBuilder<I> parent;

        public ModPrimitiveBuilder(MapBuilder<I> parent, PrimitiveTypeName type) {
          super(parent.parent, type);
          this.parent = parent;
        }

        @Override
        protected ModPrimitiveBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModPrimitiveBuilder<P> value(PrimitiveTypeName type, Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModPrimitiveBuilder<P>((MapBuilder<P>)(
            (BaseMapBuilder.MapListKeyBuilder) parent).parent, type).repetition(repetition);
      }

      public ModPrimitiveBuilder<P> requiredValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.REQUIRED);
      }

      public ModPrimitiveBuilder<P> optionalValue(PrimitiveTypeName type) {
        return value(type, Type.Repetition.OPTIONAL);
      }

      public class ModGroupBuilder<I> extends BaseGroupBuilder<I,
          ModGroupBuilder<I>> {

        MapBuilder<I> parent;

        public ModGroupBuilder(MapBuilder<I> parent) {
          super(parent.parent);
          this.parent = parent;
        }

        @Override
        protected ModGroupBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModGroupBuilder<P> groupValue(Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModGroupBuilder<P>((MapBuilder<P>)(
            (BaseMapBuilder.MapListKeyBuilder) parent).parent).repetition(repetition);
      }

      public ModGroupBuilder<P> requiredGroupValue() {
        return groupValue(Type.Repetition.REQUIRED);
      }

      public ModGroupBuilder<P> optionalGroupValue() {
        return groupValue(Type.Repetition.OPTIONAL);
      }

      public class ModMapBuilder<I> extends BaseMapBuilder<I,
          ModMapBuilder<I>> {

        MapBuilder<I> parent;

        public ModMapBuilder(MapBuilder<I> parent) {
          super(parent.parent);
          this.parent = parent;
        }

        @Override
        protected ModMapBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModMapBuilder<P> mapValue(Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModMapBuilder<P>((MapBuilder<P>)(
            (BaseMapBuilder.MapListKeyBuilder) parent).parent).repetition(repetition);
      }

      public ModMapBuilder<P> requiredMapValue() {
        return mapValue(Type.Repetition.REQUIRED);
      }

      public ModMapBuilder<P> optionalMapValue() {
        return mapValue(Type.Repetition.OPTIONAL);
      }

      public class ModListBuilder<I> extends BaseListBuilder<I,
          ModListBuilder<I>> {

        MapBuilder<I> parent;

        public ModListBuilder(MapBuilder<I> parent) {
          super(parent.parent);
          this.parent = parent;
        }

        @Override
        protected ModListBuilder<I> self() {
          return this;
        }

        @Override
        public I named(String name) {
          parent.setValueType(build("value"));
          return parent.named(name);
        }
      }

      public ModListBuilder<P> listValue(Type.Repetition repetition) {
        parent.setElementType(super.build("element"));
        parent.repetition(Type.Repetition.REQUIRED);
        ((BaseMapBuilder.MapListKeyBuilder) parent).parent.setKeyType(parent.build("key"));
        return new ModListBuilder<P>((MapBuilder<P>)(
            (BaseMapBuilder.MapListKeyBuilder) parent).parent).repetition(repetition);
      }

      public ModListBuilder<P> requiredListValue() {
        return listValue(Type.Repetition.REQUIRED);
      }

      public ModListBuilder<P> optionalListValue() {
        return listValue(Type.Repetition.OPTIONAL);
      }
    }

    @Override
    protected abstract Q self();

    @Override
    protected Type build(String name) {
      Preconditions.checkNotNull(elementType, "List element type");
      return buildGroup(repetition)
          .as(OriginalType.LIST)
          .repeatedGroup()
          .addFields(elementType)
          .named("list")
          .named(name);
    }

    public P named(String name) {
      Preconditions.checkState(originalType == null,
          "LIST is already the logical type and can't be changed");
      return super.named(name);
    }

    public ListGroupElementBuilder<P, Q> groupElement(Type.Repetition repetition) {
      Preconditions
          .checkState(elementType == null, "groupElement can be called just once on a ListBuilder");
      if (parent != null) {
        return new ListGroupElementBuilder<P, Q>(this).repetition
            (repetition);
      } else {
        return new ListGroupElementBuilder<P, Q>(this, returnType).repetition
            (repetition);
      }
    }

    public ListGroupElementBuilder<P, Q> requiredGroupElement() {
      return groupElement(Type.Repetition.REQUIRED);
    }

    public ListGroupElementBuilder<P, Q> optionalGroupElement() {
      return groupElement(Type.Repetition.OPTIONAL);
    }

    public ListElementBuilder<P, Q> element(PrimitiveTypeName type, Type.Repetition repetition) {
      Preconditions
          .checkState(elementType == null, "groupElement can be called just once on a ListBuilder");
      if (parent != null) {
        return new ListElementBuilder<P, Q>(this, type)
            .repetition(repetition);
      } else {
        return new ListElementBuilder<P, Q>(this, returnType, type)
            .repetition(repetition);
      }
    }

    public ListElementBuilder<P, Q> requiredElement(PrimitiveTypeName type) {
      return element(type, Type.Repetition.REQUIRED);
    }

    public ListElementBuilder<P, Q> optionalElement(PrimitiveTypeName type) {
      return element(type, Type.Repetition.OPTIONAL);
    }

    public ListListElementBuilder<P, Q> listElement(Type.Repetition repetition) {
      Preconditions
          .checkState(elementType == null, "groupElement can be called just once on a ListBuilder");
      if (parent != null) {
        return new ListListElementBuilder<P, Q>(this).repetition(repetition);
      } else {
        return new ListListElementBuilder<P, Q>(this, returnType).repetition(repetition);
      }
    }

    public ListListElementBuilder<P, Q> requiredListElement() {
      return listElement(Type.Repetition.REQUIRED);
    }

    public ListListElementBuilder<P, Q> optionalListElement() {
      return listElement(Type.Repetition.OPTIONAL);
    }

    public ListMapElementBuilder<P, Q> mapElement(Type.Repetition repetition) {
      Preconditions
          .checkState(elementType == null, "groupElement can be called just once on a ListBuilder");
      if (parent != null) {
        return new ListMapElementBuilder<P, Q>(this).repetition(repetition);
      } else {
        return new ListMapElementBuilder<P, Q>(this, returnType).repetition(repetition);
      }
    }

    public ListMapElementBuilder<P, Q> requiredMapElement() {
      return mapElement(Type.Repetition.REQUIRED);
    }

    public ListMapElementBuilder<P, Q> optionalMapElement() {
      return mapElement(Type.Repetition.OPTIONAL);
    }

    public ListKeyElementBuilder<P, Q> keyElement(PrimitiveTypeName type,
                                                Type.Repetition repetition) {
      Preconditions
          .checkState(elementType == null, "groupElement can be called just once on a ListBuilder");
      if (parent != null) {
        return new ListKeyElementBuilder<P, Q>(this, type)
            .repetition(repetition);
      } else {
        return new ListKeyElementBuilder<P, Q>(this, returnType, type)
            .repetition(repetition);
      }
    }

    public ListKeyGroupElementBuilder<P, Q> keyGroupElement(Type.Repetition repetition) {
      Preconditions
          .checkState(elementType == null, "groupElement can be called just once on a ListBuilder");
      if (parent != null) {
        return new ListKeyGroupElementBuilder<P, Q>(this)
            .repetition(repetition);
      } else {
        return new ListKeyGroupElementBuilder<P, Q>(this, returnType)
            .repetition(repetition);
      }
    }
  }

  public static class ListBuilder<P> extends BaseListBuilder<P, ListBuilder<P>> {
    public ListBuilder(P parent) {
      super(parent);
    }

    public ListBuilder(Class<P> returnType) {
      super(returnType);
    }

    @Override
    protected ListBuilder<P> self() {
      return this;
    }
  }

  public static class MessageTypeBuilder extends GroupBuilder<MessageType> {
    private MessageTypeBuilder() {
      super(MessageType.class);
      repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Builds and returns the {@link MessageType} configured by this builder.
     * <p>
     * <em>Note:</em> All primitive types and sub-groups should be added before
     * calling this method.
     *
     * @param name a name for the constructed type
     * @return the final {@code MessageType} configured by this builder.
     */
    @Override
    public MessageType named(String name) {
      Preconditions.checkNotNull(name, "Name is required");
      return new MessageType(name, fields);
    }
  }

  /**
   * Returns a builder to construct a {@link MessageType}.
   *
   * @return a {@link MessageTypeBuilder}
   */
  public static MessageTypeBuilder buildMessage() {
    return new MessageTypeBuilder();
  }

  public static GroupBuilder<GroupType> buildGroup(
      Type.Repetition repetition) {
    return new GroupBuilder<GroupType>(GroupType.class).repetition(repetition);
  }

  /**
   * Returns a builder to construct a required {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> requiredGroup() {
    return new GroupBuilder<GroupType>(GroupType.class)
        .repetition(Type.Repetition.REQUIRED);
  }

  /**
   * Returns a builder to construct an optional {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> optionalGroup() {
    return new GroupBuilder<GroupType>(GroupType.class)
        .repetition(Type.Repetition.OPTIONAL);
  }

  /**
   * Returns a builder to construct a repeated {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> repeatedGroup() {
    return new GroupBuilder<GroupType>(GroupType.class)
        .repetition(Type.Repetition.REPEATED);
  }

  public static PrimitiveBuilder<PrimitiveType> primitive(
      PrimitiveTypeName type, Type.Repetition repetition) {
    return new PrimitiveBuilder<PrimitiveType>(PrimitiveType.class, type)
        .repetition(repetition);
  }

  /**
   * Returns a builder to construct a required {@link PrimitiveType}.
   *
   * @param type a {@link PrimitiveTypeName} for the constructed type
   * @return a {@link PrimitiveBuilder}
   */
  public static PrimitiveBuilder<PrimitiveType> required(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(PrimitiveType.class, type)
        .repetition(Type.Repetition.REQUIRED);
  }

  /**
   * Returns a builder to construct an optional {@link PrimitiveType}.
   *
   * @param type a {@link PrimitiveTypeName} for the constructed type
   * @return a {@link PrimitiveBuilder}
   */
  public static PrimitiveBuilder<PrimitiveType> optional(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(PrimitiveType.class, type)
        .repetition(Type.Repetition.OPTIONAL);
  }

  /**
   * Returns a builder to construct a repeated {@link PrimitiveType}.
   *
   * @param type a {@link PrimitiveTypeName} for the constructed type
   * @return a {@link PrimitiveBuilder}
   */
  public static PrimitiveBuilder<PrimitiveType> repeated(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(PrimitiveType.class, type)
        .repetition(Type.Repetition.REPEATED);
  }


  public static MapBuilder<GroupType> map(Type.Repetition repetition) {
    return new MapBuilder<GroupType>(GroupType.class).repetition(repetition);
  }

  public static MapBuilder<GroupType> requiredMap() {
    return map(Type.Repetition.REQUIRED);
  }

  public static MapBuilder<GroupType> optionalMap() {
    return map(Type.Repetition.OPTIONAL);
  }


  public static ListBuilder<GroupType> list(Type.Repetition repetition) {
    return new ListBuilder<GroupType>(GroupType.class).repetition(repetition);
  }

  public static ListBuilder<GroupType> requiredList() {
    return list(Type.Repetition.REQUIRED);
  }

  public static ListBuilder<GroupType> optionalList() {
    return list(Type.Repetition.OPTIONAL);
  }

}
