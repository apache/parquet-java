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
import java.util.List;

import parquet.column.ColumnDescriptor;
import parquet.io.InvalidRecordException;
import parquet.schema.PrimitiveType.PrimitiveTypeName;


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
  *
  * @param name the name of the type
  * @param fields the fields contained by this message
  */
 public MessageType(String name, List<Type> fields) {
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
        .append(getOriginalType() == null ? "" : " (" + getOriginalType() +")")
        .append(" {\n");
    membersDisplayString(sb, "  ");
    sb.append("}\n");
  }

  /**
   * @return the max repetition level that might be needed to encode the
   * type at 'path'. 
   */
  public int getMaxRepetitionLevel(String ... path) {
    return getMaxRepetitionLevel(path, 0) - 1;
  }

  /**
   * @return the max repetition level that might be needed to encode the
   * type at 'path'. 
   */
  public int getMaxDefinitionLevel(String ... path) {
    return getMaxDefinitionLevel(path, 0) - 1;
  }

  public Type getType(String ... path) {
    return getType(path, 0);
  }

  public ColumnDescriptor getColumnDescription(String[] path) {
    int maxRep = getMaxRepetitionLevel(path);
    int maxDef = getMaxDefinitionLevel(path);
    PrimitiveTypeName type = getType(path).asPrimitiveType().getPrimitiveTypeName();
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
      columns.add(new ColumnDescriptor(path, getType(path).asPrimitiveType().getPrimitiveTypeName(), getMaxRepetitionLevel(path), getMaxDefinitionLevel(path)));
    }
    return columns;
  }

  @Override
  public void checkContains(Type subType) {
    if (!(subType instanceof MessageType)) {
      throw new InvalidRecordException(subType + " found: expected " + this);
    }
    super.checkContains(subType);
  }

  public <T> T convertWith(TypeConverter<T> converter) {
    final ArrayList<GroupType> path = new ArrayList<GroupType>();
    path.add(this);
    return converter.convertMessageType(this, convertChildren(path, converter));
  }
}
