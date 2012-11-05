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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupType extends Type {

  private final List<Type> fields;
  private final Map<String, Integer> indexByName;

  public GroupType(Repetition repetition, String name, List<Type> fields) {
    super(name, repetition);
    this.fields = fields;
    this.indexByName = new HashMap<String, Integer>();
    for (int i = 0; i < fields.size(); i++) {
      indexByName.put(fields.get(i).getName(), i);
    }
  }

  public String getFieldName(int index) {
    return fields.get(index).getName();
  }

  public boolean containsField(String name) {
    return indexByName.containsKey(name);
  }

  public int getFieldIndex(String name) {
    if (!indexByName.containsKey(name)) {
      throw new RuntimeException(name + " not found in " + this);
    }
    return indexByName.get(name);
  }

  public GroupType(Repetition required, String name, Type... fields) {
    this(required, name, Arrays.asList(fields));
  }

  public List<Type> getFields() {
    return fields;
  }

  public int getFieldCount() {
    return fields.size();
  }

  @Override
  public boolean isPrimitive() {
    return false;
  }

  public Type getType(String fieldName) {
    return getType(getFieldIndex(fieldName));
  }

  public Type getType(int index) {
    return fields.get(index);
  }

  void membersDisplayString(StringBuilder sb, String indent) {
    for (Type field : fields) {
      field.writeToStringBuilder(sb, indent);
      if (field.isPrimitive()) {
        sb.append(";");
      }
      sb.append("\n");
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    writeToStringBuilder(sb, "");
    return sb.toString();
  }

  @Override
  public void writeToStringBuilder(StringBuilder sb, String indent) {
    sb.append(indent)
        .append(getRepetition().name().toLowerCase())
        .append(" group ")
        .append(getName())
        .append(" {\n");
    membersDisplayString(sb, indent + "  ");
    sb.append(indent)
        .append("}");
  }

  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  protected int typeHashCode() {
    int c = 17;
    c += 31 * getRepetition().hashCode();
    c += 31 * getName().hashCode();
    c += 31 * getFields().hashCode();
    return c;
  }

  @Override
  protected boolean typeEquals(Type other) {
    Type otherType = (Type) other;
    if (otherType.isPrimitive()) {
      return false;
    } else {
      GroupType groupType = otherType.asGroupType();
      return getRepetition() == groupType.getRepetition() &&
          getName().equals(groupType.getName()) &&
          getFields().equals(groupType.getFields());
    }
  }
}
