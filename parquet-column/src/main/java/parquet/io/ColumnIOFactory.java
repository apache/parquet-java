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
package parquet.io;

import java.util.ArrayList;
import java.util.List;

import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.Type;
import parquet.schema.TypeVisitor;
import parquet.schema.Type.Repetition;

/**
 * Factory constructing the ColumnIO structure from the schema
 *
 * @author Julien Le Dem
 *
 */
public class ColumnIOFactory {

  public class ColumnIOCreatorVisitor implements TypeVisitor {

    private MessageColumnIO columnIO;
    private GroupColumnIO current;
    private List<PrimitiveColumnIO> leaves = new ArrayList<PrimitiveColumnIO>();
    private final boolean validating;

    public ColumnIOCreatorVisitor(boolean validating) {
      this.validating = validating;
    }

    @Override
    public void visit(GroupType groupType) {
      GroupColumnIO newIO;
      if (groupType.getRepetition() == Repetition.REPEATED) {
        newIO = new GroupColumnIO(groupType, current, current.getChildrenCount());
      } else {
        newIO = new GroupColumnIO(groupType, current, current.getChildrenCount());
      }
      current.add(newIO);
      visitChildren(newIO, groupType);
    }

    private void visitChildren(GroupColumnIO newIO, GroupType groupType) {
      GroupColumnIO oldIO = current;
      current = newIO;
      for (Type type : groupType.getFields()) {
        type.accept(this);
      }
      current = oldIO;
    }

    @Override
    public void visit(MessageType messageType) {
      columnIO = new MessageColumnIO(messageType, validating);
      visitChildren(columnIO, messageType);
      columnIO.setLevels();
      columnIO.setLeaves(leaves);
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
      PrimitiveColumnIO newIO = new PrimitiveColumnIO(primitiveType, current, current.getChildrenCount(), leaves.size());
      current.add(newIO);
      leaves.add(newIO);
    }

    public MessageColumnIO getColumnIO() {
      return columnIO;
    }

  }

  private final boolean validating;

  /**
   * validation is off by default
   */
  public ColumnIOFactory() {
    this(false);
  }

  /**
   * @param validating to turn validation on
   */
  public ColumnIOFactory(boolean validating) {
    super();
    this.validating = validating;
  }

  /**
   * @param schema the schema we want to read/write
   * @return the corresponding serializing/deserializing structure
   */
  public MessageColumnIO getColumnIO(MessageType schema) {
    ColumnIOCreatorVisitor visitor = new ColumnIOCreatorVisitor(validating);
    schema.accept(visitor);
    return visitor.getColumnIO();
  }

}
