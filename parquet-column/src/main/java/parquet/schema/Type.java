/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.schema;

import java.util.List;

import parquet.io.InvalidRecordException;

/**
 * Represents the declared type for a field in a schema.
 * The Type object represents both the actual underlying type of the object
 * (eg a primitive or group) as well as its attributes such as whether it is
 * repeated, required, or optional.
 */
abstract public class Type {

  public static enum Repetition {
    REQUIRED, // exactly 1
    OPTIONAL, // 0 or 1
    REPEATED  // 0 or more
  }

  private final String name;
  private final Repetition repetition;
  private final OriginalType originalType;

  public Type(String name, Repetition repetition) {
    this(name, repetition, null);
  }

  public Type(String name, Repetition repetition, OriginalType originalType) {
    super();
    this.name = name;
    this.repetition = repetition;
    this.originalType = originalType;
  }

  public String getName() {
    return name;
  }

  public Repetition getRepetition() {
    return repetition;
  }

  public OriginalType getOriginalType() {
    return originalType;
  }

  abstract public boolean isPrimitive();

  public GroupType asGroupType() {
    if (isPrimitive()) {
      throw new ClassCastException(this.getName() + " is not a group");
    }
    return (GroupType)this;
  }

  public PrimitiveType asPrimitiveType() {
    if (!isPrimitive()) {
      throw new ClassCastException(this.getName() + " is not a primititve");
    }
    return (PrimitiveType)this;
  }

  /**
   * Writes a string representation to the provided StringBuilder
   * @param sb the StringBuilder to write itself to
   * @param current indentation level
   */
  abstract public void writeToStringBuilder(StringBuilder sb, String indent);

  /**
   * Visits this type with the given visitor
   * @param visitor the visitor to visit this type
   */
  abstract public void accept(TypeVisitor visitor);

  @Override
  public int hashCode() {
    return typeHashCode();
  }

  protected abstract int typeHashCode();

  protected abstract boolean typeEquals(Type other);

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Type) || other == null) {
      return false;
    }
    return typeEquals((Type)other);
  }

  protected abstract int getMaxRepetitionLevel(String[] path, int i);

  protected abstract int getMaxDefinitionLevel(String[] path, int i);

  protected abstract Type getType(String[] path, int i);

  protected abstract List<String[]> getPaths(int depth);

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    writeToStringBuilder(sb, "");
    return sb.toString();
  }

  void checkContains(Type subType) {
    if (!this.name.equals(subType.name)
        || this.repetition != subType.repetition) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
  }

  /**
   *
   * @param converter logic to convert the tree
   * @return the converted tree
   */
   abstract <T> T convert(List<GroupType> path, TypeConverter<T> converter);

}
