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
package parquet.schema;

import java.util.ArrayList;
import java.util.List;

import parquet.Preconditions;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.ID;

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
        // if the parent is a GroupBuilder, add type to it
        if (GroupBuilder.class.isAssignableFrom(parent.getClass())) {
          GroupBuilder.class.cast(parent).addField(type);
        }
        return parent;
      } else {
        // no parent indicates that the Type object should be returned
        // the constructor check guarantees that returnClass is a Type
        return returnClass.cast(type);
      }
    }

  }

  /**
   * A builder for {@link PrimitiveType} objects.
   *
   * @param <P> The type that this builder will return from
   *          {@link #named(String)} when the type is built.
   */
  public static class PrimitiveBuilder<P> extends Builder<PrimitiveBuilder<P>, P> {
    private static final long MAX_PRECISION_INT32 = maxPrecision(4);
    private static final long MAX_PRECISION_INT64 = maxPrecision(8);
    private final PrimitiveTypeName primitiveType;
    private int length = NOT_SET;
    private int precision = NOT_SET;
    private int scale = NOT_SET;

    private PrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent);
      this.primitiveType = type;
    }

    private PrimitiveBuilder(Class<P> returnType, PrimitiveTypeName type) {
      super(returnType);
      this.primitiveType = type;
    }

    @Override
    protected PrimitiveBuilder<P> self() {
      return this;
    }

    /**
     * Adds the length for a FIXED_LEN_BYTE_ARRAY.
     *
     * @param length an int length
     * @return this builder for method chaining
     */
    public PrimitiveBuilder<P> length(int length) {
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
    public PrimitiveBuilder<P> precision(int precision) {
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
    public PrimitiveBuilder<P> scale(int scale) {
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
   * A builder for {@link GroupType} objects.
   *
   * @param <P> The type that this builder will return from
   *          {@link #named(String)} when the type is built.
   */
  public static class GroupBuilder<P> extends Builder<GroupBuilder<P>, P> {
    protected final List<Type> fields;

    private GroupBuilder(P parent) {
      super(parent);
      this.fields = new ArrayList<Type>();
    }

    private GroupBuilder(Class<P> returnType) {
      super(returnType);
      this.fields = new ArrayList<Type>();
    }

    @Override
    protected GroupBuilder<P> self() {
      return this;
    }

    public PrimitiveBuilder<GroupBuilder<P>> primitive(
        PrimitiveTypeName type, Type.Repetition repetition) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
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
    public PrimitiveBuilder<GroupBuilder<P>> required(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
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
    public PrimitiveBuilder<GroupBuilder<P>> optional(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
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
    public PrimitiveBuilder<GroupBuilder<P>> repeated(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
          .repetition(Type.Repetition.REPEATED);
    }

    public GroupBuilder<GroupBuilder<P>> group(Type.Repetition repetition) {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(repetition);
    }

    /**
     * Returns a {@link GroupBuilder} to build a required sub-group.
     *
     * @return a group builder that will return this builder for additional
     *          fields.
     */
    public GroupBuilder<GroupBuilder<P>> requiredGroup() {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Returns a {@link GroupBuilder} to build an optional sub-group.
     *
     * @return a group builder that will return this builder for additional
     *          fields.
     */
    public GroupBuilder<GroupBuilder<P>> optionalGroup() {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(Type.Repetition.OPTIONAL);
    }

    /**
     * Returns a {@link GroupBuilder} to build a repeated sub-group.
     *
     * @return a group builder that will return this builder for additional
     *          fields.
     */
    public GroupBuilder<GroupBuilder<P>> repeatedGroup() {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(Type.Repetition.REPEATED);
    }

    /**
     * Adds {@code type} as a sub-field to the group configured by this builder.
     *
     * @return this builder for additional fields.
     */
    public GroupBuilder<P> addField(Type type) {
      fields.add(type);
      return this;
    }

    /**
     * Adds {@code types} as sub-fields of the group configured by this builder.
     *
     * @return this builder for additional fields.
     */
    public GroupBuilder<P> addFields(Type... types) {
      for (Type type : types) {
        fields.add(type);
      }
      return this;
    }

    @Override
    protected GroupType build(String name) {
      Preconditions.checkState(!fields.isEmpty(),
          "Cannot build an empty group");
      return new GroupType(repetition, name, originalType, fields, id);
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

}
