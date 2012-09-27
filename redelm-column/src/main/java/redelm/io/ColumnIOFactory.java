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
package redelm.io;

import java.util.ArrayList;
import java.util.List;

import redelm.column.ColumnsStore;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;
import redelm.schema.Type;
import redelm.schema.Type.Repetition;
import redelm.schema.TypeVisitor;

public class ColumnIOFactory {

  public class ColumnIOCreatorVisitor implements TypeVisitor {

    private MessageColumnIO columnIO;
    private GroupColumnIO current;
    private final ColumnsStore columnStore;
    private List<PrimitiveColumnIO> leaves = new ArrayList<PrimitiveColumnIO>();

    public ColumnIOCreatorVisitor(ColumnsStore columnStore) {
      this.columnStore = columnStore;
    }

    @Override
    public void visit(GroupType groupType) {
      GroupColumnIO newIO;
      if (groupType.getRepetition() == Repetition.REPEATED) {
        newIO = new GroupColumnIO(groupType, current);
      } else {
        newIO = new GroupColumnIO(groupType, current);
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
      columnIO = new MessageColumnIO(messageType);
      visitChildren(columnIO, messageType);
      columnIO.setLevels(columnStore);
      columnIO.setLeaves(leaves);
    }

    @Override
    public void visit(PrimitiveType primitiveType) {
      PrimitiveColumnIO newIO = new PrimitiveColumnIO(primitiveType, current);
      current.add(newIO);
      leaves.add(newIO);
    }

    public MessageColumnIO getColumnIO() {
      return columnIO;
    }

  }

  public ColumnIOFactory() {
    super();
  }

  public MessageColumnIO getColumnIO(MessageType schema, ColumnsStore columnStore) {
    ColumnIOCreatorVisitor visitor = new ColumnIOCreatorVisitor(columnStore);
    schema.accept(visitor);
    return visitor.getColumnIO();
  }

}
