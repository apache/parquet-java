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

import java.util.ArrayList;
import java.util.List;

import redelm.column.ColumnDescriptor;
import redelm.schema.PrimitiveType.Primitive;

/**
 * The root of a schema
 *
 * @author Julien Le Dem
 *
 */
public class MessageType extends GroupType {

  /**
   *
   * @param name the name of the type
   * @param fields the fields contained by this message
   */
  public MessageType(String name, Type... fields) {
    super(Repetition.REPEATED, name, fields);
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
  public void writeToStringBuilder(StringBuilder sb, String indent) {
    sb.append("message ")
        .append(getName())
        .append(" {\n");
    membersDisplayString(sb
        , "  ");
    sb.append("}\n");
  }

  public int getRepetitionLevel(String[] path) {
    return getRepetitionLevel(path, 0) - 1;
  }

  public int getDefinitionLevel(String[] path) {
    return getDefinitionLevel(path, 0) - 1;
  }

  public Type getType(String[] path) {
    return getType(path, 0);
  }

  public ColumnDescriptor getColumnDescription(String[] path) {
    int maxRep = getRepetitionLevel(path);
    int maxDef = getDefinitionLevel(path);
    Primitive type = getType(path).asPrimitiveType().getPrimitive();
    return new ColumnDescriptor(path, type, maxRep, maxDef);
  }

  public List<String[]> getPaths() {
    return this.getPaths(0);
  }

  public List<ColumnDescriptor> getColumns() {
    List<String[]> paths = this.getPaths(0);
    List<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>(paths.size());
    for (String[] path : paths) {
      // TODO: optimize this
      columns.add(new ColumnDescriptor(path, getType(path).asPrimitiveType().getPrimitive(), getRepetitionLevel(path), getDefinitionLevel(path)));
    }
    return columns;
  }
}
