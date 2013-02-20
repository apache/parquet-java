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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a group type: a list of fields
 *
 * @author Julien Le Dem
 *
 */
public class GroupType extends Type {

  private final List<Type> fields;
  private final Map<String, Integer> indexByName;

  /**
   * @param repetition OPTIONAL, REPEATED, REQUIRED
   * @param name the name of the field
   * @param fields the contained fields
   */
  public GroupType(Repetition repetition, String name, List<Type> fields) {
    super(name, repetition);
    this.fields = fields;
    this.indexByName = new HashMap<String, Integer>();
    for (int i = 0; i < fields.size(); i++) {
      indexByName.put(fields.get(i).getName(), i);
    }
  }

  /**
   * @see GroupType#GroupType(Repetition, String, List)
   * @param required
   * @param name
   * @param fields
   */
  public GroupType(Repetition required, String name, Type... fields) {
    this(required, name, Arrays.asList(fields));
  }

  /**
   * returns the name of the corresponding field
   * @param index the index of the desired field in this type
   * @return the name of the field at this index
   */
  public String getFieldName(int index) {
    return fields.get(index).getName();
  }

  /**
   * @param name the requested name
   * @return whether this type contains a field with that name
   */
  public boolean containsField(String name) {
    return indexByName.containsKey(name);
  }

  /**
   *
   * @param name
   * @return the index of the field with that name
   */
  public int getFieldIndex(String name) {
    if (!indexByName.containsKey(name)) {
      throw new RuntimeException(name + " not found in " + this);
    }
    return indexByName.get(name);
  }

  /**
   * @return the fields contained in this type
   */
  public List<Type> getFields() {
    return fields;
  }

  /**
   * @return the number of fields in this type
   */
  public int getFieldCount() {
    return fields.size();
  }

  /**
   * @return false
   */
  @Override
  public boolean isPrimitive() {
    return false;
  }

  /**
   * @param fieldName
   * @return the type of this field by name
   */
  public Type getType(String fieldName) {
    return getType(getFieldIndex(fieldName));
  }

  /**
   * @param index
   * @return the type of this field by index
   */
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

  /**
   * {@inheritDoc}
   */
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected int typeHashCode() {
    int c = 17;
    c += 31 * getRepetition().hashCode();
    c += 31 * getName().hashCode();
    c += 31 * getFields().hashCode();
    return c;
  }

  /**
   * {@inheritDoc}
   */
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

  @Override
  protected int getRepetitionLevel(String[] path, int i) {
    int myVal = getRepetition() == Repetition.REPEATED ? 1 : 0;
    if (i == path.length) {
      return myVal;
    }
    return myVal + getType(path[i++]).getRepetitionLevel(path, i);
  }

  @Override
  protected int getDefinitionLevel(String[] path, int i) {
    int myVal = getRepetition() != Repetition.REQUIRED ? 1 : 0;
    if (i == path.length) {
      return myVal;
    }
    return myVal + getType(path[i++]).getDefinitionLevel(path, i);
  }

  @Override
  protected Type getType(String[] path, int i) {
    if (i == path.length) {
      return this;
    }
    return getType(path[i++]).getType(path, i);
  }

  @Override
  protected List<String[]> getPaths(int depth) {
    List<String[]> result = new ArrayList<String[]>();
    for (Type field : fields) {
      List<String[]> paths = field.getPaths(depth + 1);
      for (String[] path : paths) {
        path[depth] = field.getName();
        result.add(path);
      }
    }
    return result;
  }

  @Override
  void checkContains(Type subType) {
    super.checkContains(subType);
    if (subType.isPrimitive()) {
      throw new RuntimeException(subType + " found: expected " + this);
    }
    List<Type> fields = subType.asGroupType().getFields();
    for (Type otherType : fields) {
      Type thisType = this.getType(otherType.getName());
      thisType.checkContains(otherType);
    }
  }

}
