package parquet.schema;

import java.util.ArrayList;
import java.util.List;
import parquet.Preconditions;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class Types {
  private static final int NOT_SET = 0;

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

    public final T repetition(Type.Repetition repetition) {
      Preconditions.checkArgument(!repetitionAlreadySet,
          "Repetition has already been set");
      Preconditions.checkNotNull(repetition, "Repetition cannot be null");
      this.repetition = repetition;
      this.repetitionAlreadySet = true;
      return this.asCorrectType;
    }

    public T as(OriginalType type) {
      this.originalType = type;
      return this.asCorrectType;
    }

    /**
     * Adds the precision param to a DECIMAL. Must be called after
     * {@link #as(OriginalType)}.
     */
    public T precision(int precision) {
      this.precision = precision;
      return this.asCorrectType;
    }

    /**
     * Adds the scale param to a DECIMAL. Must be called after
     * {@link #as(OriginalType)}.
     */
    public T scale(int scale) {
      this.scale = scale;
      return this.asCorrectType;
    }

    abstract protected Type build(String name);

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
        meta = new OriginalTypeMeta(precision, scale);
      }
      return meta;
    }
  }

  public static class PrimitiveBuilder<P> extends Builder<PrimitiveBuilder<P>, P> {
    private final PrimitiveTypeName primitiveType;
    private int length = NOT_SET;

    private PrimitiveBuilder(P parent, PrimitiveTypeName type) {
      super(parent);
      this.primitiveType = type;
    }

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

  public static class GroupBuilder<P> extends Builder<GroupBuilder<P>, P> {
    protected final List<Type> fields;

    private GroupBuilder(P parent) {
      super(parent);
      this.fields = new ArrayList<Type>();
    }

    public PrimitiveBuilder<GroupBuilder<P>> primitive(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type);
    }

    public PrimitiveBuilder<GroupBuilder<P>> required(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
          .repetition(Type.Repetition.REQUIRED);
    }

    public PrimitiveBuilder<GroupBuilder<P>> optional(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
          .repetition(Type.Repetition.OPTIONAL);
    }

    public PrimitiveBuilder<GroupBuilder<P>> repeated(
        PrimitiveTypeName type) {
      return new PrimitiveBuilder<GroupBuilder<P>>(this, type)
          .repetition(Type.Repetition.REPEATED);
    }

    public GroupBuilder<GroupBuilder<P>> group() {
      return new GroupBuilder<GroupBuilder<P>>(this);
    }

    public GroupBuilder<GroupBuilder<P>> requiredGroup() {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(Type.Repetition.REQUIRED);
    }

    public GroupBuilder<GroupBuilder<P>> optionalGroup() {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(Type.Repetition.OPTIONAL);
    }

    public GroupBuilder<GroupBuilder<P>> repeatedGroup() {
      return new GroupBuilder<GroupBuilder<P>>(this)
          .repetition(Type.Repetition.REPEATED);
    }

    public GroupBuilder<P> addField(Type type) {
      fields.add(type);
      return this;
    }

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

    @Override
    public MessageType named(String name) {
      Preconditions.checkNotNull(name, "Name is required");
      // TODO: this causes parquet-thrift testNotPullInOptionalFields to fail
      //Preconditions.checkState(!fields.isEmpty(),
      //    "Cannot build an empty message");
      return new MessageType(name, fields);
    }
  }

  public static MessageTypeBuilder buildMessage() {
    return new MessageTypeBuilder();
  }

  public static GroupBuilder<GroupType> buildGroup() {
    return new GroupBuilder<GroupType>(null);
  }

  public static GroupBuilder<GroupType> requiredGroup() {
    return new GroupBuilder<GroupType>(null)
        .repetition(Type.Repetition.REQUIRED);
  }

  public static GroupBuilder<GroupType> optionalGroup() {
    return new GroupBuilder<GroupType>(null)
        .repetition(Type.Repetition.OPTIONAL);
  }

  public static GroupBuilder<GroupType> repeatedGroup() {
    return new GroupBuilder<GroupType>(null)
        .repetition(Type.Repetition.REPEATED);
  }

  public static PrimitiveBuilder<PrimitiveType> primitive(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(null, type);
  }

  public static PrimitiveBuilder<PrimitiveType> required(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(null, type)
        .repetition(Type.Repetition.REQUIRED);
  }

  public static PrimitiveBuilder<PrimitiveType> optional(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(null, type)
        .repetition(Type.Repetition.OPTIONAL);
  }

  public static PrimitiveBuilder<PrimitiveType> repeated(
      PrimitiveTypeName type) {
    return new PrimitiveBuilder<PrimitiveType>(null, type)
        .repetition(Type.Repetition.REPEATED);
  }

}
