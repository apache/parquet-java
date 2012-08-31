package redelm.schema;

import java.util.Arrays;

abstract public class Type {

  public static enum Repetition {
    REQUIRED, // exactly 1
    OPTIONAL, // 0 or 1
    REPEATED  // 0 or more
  }

  private final String name;
  private final Repetition repetition;
  private String[] fieldPath;

  public Type(String name, Repetition repeatition) {
    super();
    this.name = name;
    this.repetition = repeatition;
  }

  public String getName() {
    return name;
  }

  public Repetition getRepetition() {
    return repetition;
  }

  abstract public boolean isPrimitive();

  public GroupType asGroupType() {
    if (isPrimitive()) {
      throw new ClassCastException(this + " is not a group");
    }
    return (GroupType)this;
  }

  public PrimitiveType asPrimitiveType() {
    if (!isPrimitive()) {
      throw new ClassCastException(this.getName() + " is not a primititve");
    }
    return (PrimitiveType)this;
  }

  void setFieldPath(String[] fieldPath) {
    this.fieldPath = fieldPath;
  }

  public String[] getFieldPath() {
    return fieldPath;
  }

  abstract public String toString(String indent);

  abstract public void accept(TypeVisitor visitor);

  @Override
  public String toString() {
    return "Type [name=" + name + ", repetition=" + repetition + ", fieldPath="
        + Arrays.toString(fieldPath) + "]";
  }

}
