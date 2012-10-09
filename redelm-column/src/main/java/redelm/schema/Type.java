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

  abstract public String toString(String indent);

  abstract public void accept(TypeVisitor visitor);

  @Override
  public String toString() {
    return "Type [name=" + name + ", repetition=" + repetition + ", fieldPath="
        + Arrays.toString(fieldPath) + "]";
  }

}
