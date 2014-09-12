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

import static parquet.Preconditions.checkNotNull;

import java.util.List;

import parquet.Preconditions;
import parquet.io.InvalidRecordException;

/**
 * Represents the declared type for a field in a schema.
 * The Type object represents both the actual underlying type of the object
 * (eg a primitive or group) as well as its attributes such as whether it is
 * repeated, required, or optional.
 */
abstract public class Type {

  /**
   * Constraint on the repetition of a field
   *
   * @author Julien Le Dem
   */
  public static enum Repetition {
    /**
     * exactly 1
     */
    REQUIRED {
      @Override
      public boolean isMoreRestrictiveThan(Repetition other) {
        return other != REQUIRED;
      }
    },
    /**
     * 0 or 1
     */
    OPTIONAL {
      @Override
      public boolean isMoreRestrictiveThan(Repetition other) {
        return other == REPEATED;
      }
    },
    /**
     * 0 or more
     */
    REPEATED {
      @Override
      public boolean isMoreRestrictiveThan(Repetition other) {
        return false;
      }
    }
    ;

    /**
     * @param other
     * @return true if it is strictly more restrictive than other
     */
    abstract public boolean isMoreRestrictiveThan(Repetition other);

  }

  private final String name;
  private final Repetition repetition;
  private final OriginalType originalType;
  private final Integer id;

  /**
   * @param name the name of the type
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   */
  public Type(String name, Repetition repetition) {
    this(name, repetition, null);
  }

  /**
   * @param name the name of the type
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   */
  public Type(String name, Repetition repetition, OriginalType originalType) {
    this(name, repetition, originalType, null);
  }

  /**
   * @param name the name of the type
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param originalType (optional) the original type to help with cross schema conversion (LIST, MAP, ...)
   * @param id (optional) the id of the fields.
   */
  Type(String name, Repetition repetition, OriginalType originalType, Integer id) {
    super();
    this.name = checkNotNull(name, "name");
    this.repetition = checkNotNull(repetition, "repetition");
    this.originalType = originalType;
    this.id = id;
  }

  public abstract Type withId(int id);

  /**
   * @return the name of the type
   */
  public String getName() {
    return name;
  }

  /**
   * @param rep
   * @return if repetition of the type is rep
   */
  public boolean isRepetition(Repetition rep) {
    return repetition == rep;
  }

  /**
   * @return the repetition constraint
   */
  public Repetition getRepetition() {
    return repetition;
  }

  /**
   * @return the id of the field (if defined)
   */
  public Integer getId() {
    return id;
  }

  /**
   * @return the original type (LIST, MAP, ...)
   */
  public OriginalType getOriginalType() {
    return originalType;
  }

  /**
   * @return if this is a primitive type
   */
  abstract public boolean isPrimitive();

  /**
   * @return this if it's a group type
   * @throws ClassCastException if not
   */
  public GroupType asGroupType() {
    if (isPrimitive()) {
      throw new ClassCastException(this + " is not a group");
    }
    return (GroupType)this;
  }

  /**
   * @return this if it's a primitive type
   * @throws ClassCastException if not
   */
  public PrimitiveType asPrimitiveType() {
    if (!isPrimitive()) {
      throw new ClassCastException(this + " is not primitive");
    }
    return (PrimitiveType)this;
  }

  /**
   * Writes a string representation to the provided StringBuilder
   * @param sb the StringBuilder to write itself to
   * @param indent indentation level
   */
  abstract public void writeToStringBuilder(StringBuilder sb, String indent);

  /**
   * Visits this type with the given visitor
   * @param visitor the visitor to visit this type
   */
  abstract public void accept(TypeVisitor visitor);

  @Override
  public int hashCode() {
    int c = repetition.hashCode();
    c = 31 * c + name.hashCode();
    if (originalType != null) {
      c = 31 * c +  originalType.hashCode();
    }
    if (id != null) {
      c = 31 * c + id.hashCode();
    }
    return c;
  }

  protected boolean equals(Type other) {
    return
        name.equals(other.name)
        && repetition == other.repetition
        && eqOrBothNull(repetition, other.repetition)
        && eqOrBothNull(id, other.id);
  };

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Type) || other == null) {
      return false;
    }
    return equals((Type)other);
  }

  protected boolean eqOrBothNull(Object o1, Object o2) {
    return (o1 == null && o2 == null) || (o1 != null && o1.equals(o2));
  }

  protected abstract int getMaxRepetitionLevel(String[] path, int i);

  protected abstract int getMaxDefinitionLevel(String[] path, int i);

  protected abstract Type getType(String[] path, int i);

  protected abstract List<String[]> getPaths(int depth);

  protected abstract boolean containsPath(String[] path, int depth);

  /**
   * @param toMerge the type to merge into this one
   * @return the union result of merging toMerge into this
   */
  protected abstract Type union(Type toMerge);

  /**
   * @param toMerge the type to merge into this one
   * @param strict should schema primitive types match
   * @return the union result of merging toMerge into this
   */
  protected abstract Type union(Type toMerge, boolean strict);

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
