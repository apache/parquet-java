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

  public GroupType(Repetition repeatition, String name, List<Type> fields) {
    super(name, repeatition);
    this.fields = fields;
    this.indexByName = new HashMap<String, Integer>();
    for (int i = 0; i < fields.size(); i++) {
      indexByName.put(fields.get(i).getName(), i);
    }
  }

  public String getFieldName(int index) {
    return fields.get(index).getName();
  }

  public int getFieldIndex(String name) {
    if (!indexByName.containsKey(name)) {
      throw new RuntimeException(name + " not found in " + this);
    }
    return indexByName.get(name);
  }

  @Override
  void setFieldPath(String[] fieldPath) {
    super.setFieldPath(fieldPath);
    for (Type type : this.fields) {
      String[] newFieldPath = Arrays.copyOf(fieldPath, fieldPath.length + 1);
      newFieldPath[fieldPath.length] =  type.getName();
      type.setFieldPath(newFieldPath);
    }
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

  String membersDisplayString(String indent) {
    String string = "";
    for (Type field : fields) {
      string += field.toString(indent)+";\n";
    }
    return string;
  }

  @Override
  public String toString() {
    return toString("");
  }

  @Override
  public String toString(String indent) {
    return indent+getRepetition().name().toLowerCase()+" group "+getName()+" {\n"
        +membersDisplayString(indent+"  ")
        +indent+"}";
  }

  @Override
  public void accept(TypeVisitor visitor) {
    visitor.visit(this);
  }
}
