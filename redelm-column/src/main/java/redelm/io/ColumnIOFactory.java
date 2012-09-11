package redelm.io;

import java.util.ArrayList;
import java.util.List;

import redelm.column.ColumnsStore;
import redelm.data.GroupFactory;
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
