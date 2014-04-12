package parquet.schema;

import java.util.ArrayList;
import java.util.List;
import parquet.Preconditions;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * This class provides fluent builders that produce Parquet schema Types.
 *
 * The most basic use is to build primitive types:
 * <pre>
 *   Types.required(INT64).named("id");
 *   Types.optional(INT32).named("number");
 * </pre>
 *
 * The {@link #required(PrimitiveTypeName)} factory method produces a primitive
 * type builder, and the {@link PrimitiveBuilder#named(String)} builds the
 * {@link PrimitiveType}. Between {@code required} and {@code named}, other
 * builder methods can be used to add type annotations or other type metadata:
 * <pre>
 *   Types.required(BINARY).as(UTF8).named("username");
 *   Types.optional(FIXED_LEN_BYTE_ARRAY).length(20).named("sha1");
 * </pre>
 *
 * Optional types are built using {@link #optional(PrimitiveTypeName)} to get
 * the builder.
 *
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
 *
 * When {@code required} is called on a group builder, the builder it returns
 * will add the type to the parent group when it is built and {@code named} will
 * return its parent group builder (instead of the type) so more fields can be
 * added.
 *
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
 *
 * Message types are built using {@link #buildMessage()} and function just like
 * group builders.
 * <pre>
 *   // message User {
 *   //   required group user {
 *   //     required int64 id;
 *   //     optional binary email (UTF8);
 *   //     optional group address {
 *   //       required binary street (UTF8);
 *   //       required int32 zipcode;
 *   //     }
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
 *
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
    protected final T asCorrectType;

    protected Type.Repetition repetition = null;
    protected OriginalType originalType = null;
    private boolean repetitionAlreadySet = false;
    private int precision = NOT_SET;
    private int scale = NOT_SET;

    @SuppressWarnings("unchecked")
    protected Builder(P parent) {
      this.parent = parent;
      this.asCorrectType = (T) this;
    }

    protected final T repetition(Type.Repetition repetition) {
      Preconditions.checkArgument(!repetitionAlreadySet,
          "Repetition has already been set");
      Preconditions.checkNotNull(repetition, "Repetition cannot be null");
      this.repetition = repetition;
      this.repetitionAlreadySet = true;
      return this.asCorrectType;
    }

    /**
     * Adds a type annotation ({@link OriginalType}) to the type being built.
     *
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
      return this.asCorrectType;
    }

    /**
     * Adds the precision for a DECIMAL.
     *
     * This value is required for decimals and must be less than or equal to
     * the maximum number of base-10 digits in the underlying type. A 4-byte
     * fixed, for example, can store up to 9 base-10 digits.
     *
     * @param precision an int precision value for the DECIMAL
     * @return this builder for method chaining
     */
    public T precision(int precision) {
      this.precision = precision;
      return this.asCorrectType;
    }

    /**
     * Adds the scale for a DECIMAL.
     *
     * This value must be less than the maximum precision of the type and must
     * be a positive number. If not set, the default scale is 0.
     *
     * The scale specifies the number of digits of the underlying unscaled
     * that are to the right of the decimal point. The decimal interpretation
     * of values in this column is: {@code value*10^(-scale)}.
     *
     * @param scale an int scale value for the DECIMAL
     * @return this builder for method chaining
     */
    public T scale(int scale) {
      this.scale = scale;
      return this.asCorrectType;
    }

    abstract protected Type build(String name);

    /**
     * Builds a {@link Type} and returns the parent {@link GroupBuilder} so
     * more types can be added to it. If there is no parent builder, then the
     * constructed {@code Type} is returned.
     *
     * @param name a name for the constructed type
     * @return the parent {@code GroupBuilder} or the constructed {@code Type}
     */
    @SuppressWarnings("unchecked")
    public P named(String name) {
      Preconditions.checkNotNull(name, "Name is required");
      Preconditions.checkNotNull(repetition, "Repetition is required");

      Type type = build(name);
      if (parent != null && parent instanceof GroupBuilder) {
        ((GroupBuilder) parent).addField(type);
        return parent;
      } else {
        // no parent indicates that the Type object should be returned
        return (P) type;
      }
    }

    protected OriginalTypeMeta attachedMetadata() {
      OriginalTypeMeta meta = null;
      if (OriginalType.DECIMAL == originalType) {
        Preconditions.checkArgument(precision > 0,
            "Invalid DECIMAL precision: " + precision);
        Preconditions.checkArgument(scale >= 0,
            "Invalid DECIMAL scale: " + scale);
        Preconditions.checkArgument(scale <= precision,
            "Invalid DECIMAL scale: cannot be greater than precision");
        meta = new OriginalTypeMeta(precision, scale);
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
  public static class PrimitiveBuilder<P> extends Builder<PrimitiveBuilder<P>, P> {
    private final PrimitiveTypeName primitiveType;
    private int length = NOT_SET;

    private PrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent);
      this.primitiveType = type;
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

    @Override
    protected PrimitiveType build(String name) {
      if (PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY == primitiveType) {
        Preconditions.checkArgument(length > 0,
            "Invalid FIXED_LEN_BYTE_ARRAY length: " + length);
      }

      OriginalTypeMeta meta = attachedMetadata();

      // validate type annotations and required metadata
      if (originalType != null) {
        switch (originalType) {
          case UTF8:
            Preconditions.checkState(
                primitiveType == PrimitiveTypeName.BINARY,
                "UTF8 can only annotate binary fields");
            break;

          case DECIMAL:
            Preconditions.checkState(
                (primitiveType == PrimitiveTypeName.BINARY) ||
                (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY),
                "DECIMAL can only annotate BINARY or FIXED fields"
            );
            if (primitiveType == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
              Preconditions.checkState(
                  meta.getPrecision() <= maxPrecision(length),
                  "FIXED(" + length + ") is not long enough to store " +
                  meta.getPrecision() + " digits"
              );
            }
        }
      }

      return new PrimitiveType(
          repetition, primitiveType, length, name, originalType, meta);
    }

    private static long maxPrecision(int numBytes) {
      return Math.round(                  // convert double to long
          Math.floor(Math.log10(          // number of base-10 digits
          Math.pow(2, 8 * numBytes) - 1)  // maximum value stored in numBytes
          ));
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
      return new GroupType(
          repetition, name, originalType, attachedMetadata(), fields);
    }
  }

  public static class MessageTypeBuilder extends GroupBuilder<MessageType> {
    private MessageTypeBuilder() {
      super(null);
      repetition(Type.Repetition.REQUIRED);
    }

    /**
     * Builds and returns the {@link MessageType} configured by this builder.
     *
     * @param name a name for the constructed type
     * @return the final {@code MessageType} configured by this builder.
     */
    @Override
    public MessageType named(String name) {
      Preconditions.checkNotNull(name, "Name is required");
      // TODO: this causes parquet-thrift testNotPullInOptionalFields to fail
      //Preconditions.checkState(!fields.isEmpty(),
      //    "Cannot build an empty message");
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
    return new GroupBuilder<GroupType>(null).repetition(repetition);
  }

  /**
   * Returns a builder to construct a required {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> requiredGroup() {
    return new GroupBuilder<GroupType>(null)
        .repetition(Type.Repetition.REQUIRED);
  }

  /**
   * Returns a builder to construct an optional {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> optionalGroup() {
    return new GroupBuilder<GroupType>(null)
        .repetition(Type.Repetition.OPTIONAL);
  }

  /**
   * Returns a builder to construct a repeated {@link GroupType}.
   *
   * @return a {@link GroupBuilder}
   */
  public static GroupBuilder<GroupType> repeatedGroup() {
    return new GroupBuilder<GroupType>(null)
        .repetition(Type.Repetition.REPEATED);
  }

  public static PrimitiveBuilder<PrimitiveType> primitive(
      PrimitiveTypeName type, Type.Repetition repetition) {
    return new PrimitiveBuilder<PrimitiveType>(null, type)
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
    return new PrimitiveBuilder<PrimitiveType>(null, type)
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
    return new PrimitiveBuilder<PrimitiveType>(null, type)
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
    return new PrimitiveBuilder<PrimitiveType>(null, type)
        .repetition(Type.Repetition.REPEATED);
  }

}
