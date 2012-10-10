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

  public Type(String name, Repetition repetition) {
    super();
    this.name = name;
    this.repetition = repetition;
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

  abstract public StringBuilder toString(String indent);

  abstract public void accept(TypeVisitor visitor);

  @Override
  public String toString() {
    return new StringBuilder("Type [name=")
            .append(name)
            .append(", repetition=")
            .append(repetition)
            .append(", fieldPath=")
            .append(Arrays.toString(fieldPath))
            .append("]").toString();
  }

  @Override
  public boolean equals(Object other) {
      if (!(other instanceof Type) || other == null) {
          return false;
      }
      if (this == other) {
          return true;
      }
      Type otherType = (Type) other;
      if (isPrimitive()) {
          if (otherType.isPrimitive()) {
              return repetition == otherType.getRepetition() &&
                     name.equals(otherType.getName());
          } else {
              return false;
          }
      } else {
          if (otherType.isPrimitive()) {
              return false;
          } else {
              return repetition == otherType.getRepetition() &&
                     name.equals(otherType.getName()) &&
                     asGroupType().getFields().equals(otherType.asGroupType().getFields());
          }
      }
  }

  @Override
  public int hashCode() {
      int c = repetition.hashCode();
      c = c * 31 + name.hashCode();
      if (!isPrimitive()) {
          for (Type type : asGroupType().getFields()) {
              c = c * 31 + type.hashCode();
          }
      }
      return c;
  }
}