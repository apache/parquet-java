package redelm.io;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

import redelm.column.ColumnReader;
import redelm.data.Group;
import redelm.data.GroupFactory;

public class RecordReader {

  private static final boolean DEBUG = ColumnIO.DEBUG;

  private final PrimitiveColumnIO[] leaves;
  private final ColumnReader[] columns;
  private final int[][] nextReader;
  private final GroupFactory groupFactory;
  private final MessageColumnIO root;
  private final int[][] nextLevel;

  public RecordReader(MessageColumnIO root, List<PrimitiveColumnIO> leaves, GroupFactory groupFactory) {
    this.root = root;
    this.groupFactory = groupFactory;
    this.leaves = leaves.toArray(new PrimitiveColumnIO[leaves.size()]);
    this.columns = new ColumnReader[leaves.size()];
    this.nextReader = new int[leaves.size()][];
    this.nextLevel = new int[leaves.size()][];
    int[] firsts  = new int[16];
    // build the automaton
    for (int i = 0; i < this.leaves.length; i++) {
      PrimitiveColumnIO primitiveColumnIO = this.leaves[i];
      this.columns[i] = primitiveColumnIO.getColumnReader();
      int repetitionLevel = primitiveColumnIO.getRepetitionLevel();
      this.nextReader[i] = new int[repetitionLevel+1];
      this.nextLevel[i] = new int[repetitionLevel+1];
      for (int r = 0; r <= repetitionLevel; ++r) {
        if (primitiveColumnIO.isFirst(r)) {
          firsts[r] = i;
        }
        int next;
        if (r == 0) {
          next = i + 1;
        } else if (primitiveColumnIO.isLast(r)) {
          next = firsts[r];
        } else {
          next = i + 1;
        }
        if (primitiveColumnIO.isLast(r)) {
          ColumnIO parent = primitiveColumnIO.getParent(r);
          nextLevel[i][r] = parent.getFieldPath().length - 1;
        } else {
          nextLevel[i][r] = getCommonParentLevel(
              primitiveColumnIO.getFieldPath(),
              this.leaves[next].getFieldPath());
        }
        if (nextLevel[i][r]>this.leaves[i].getFieldPath().length-1) {
          throw new RuntimeException(Arrays.toString(this.leaves[i].getFieldPath())+" -("+r+")-> "+nextLevel[i][r]);
        }
        this.nextReader[i][r] = next;
      }
    }
  }

  private static class SimpleGroupRecordConsumer extends RecordConsumer {

    Deque<Group> groups = new ArrayDeque<Group>();
    Deque<String> fields = new ArrayDeque<String>();

    public SimpleGroupRecordConsumer(Group root) {
      groups.push(root);
    }

    @Override
    public void startField(String field) {
      fields.push(field);
    }

    @Override
    public void startGroup() {
      groups.push(groups.peek().addGroup(fields.peek()));
    }

    @Override
    public void addInt(int value) {
      groups.peek().add(fields.peek(), value);
    }

    @Override
    public void addString(String value) {
      groups.peek().add(fields.peek(), value);
    }

    @Override
    public void addBoolean(boolean value) {
      groups.peek().add(fields.peek(), value);
    }

    @Override
    public void addBinary(byte[] value) {
      groups.peek().add(fields.peek(), value);
    }

    @Override
    public void endGroup() {
      groups.pop();
    }

    @Override
    public void endField(String field) {
      fields.pop();
    }

  }

  public Group read() {
    Group newGroup = groupFactory.newGroup();
    RecordConsumer recordConsumer = new SimpleGroupRecordConsumer(newGroup);
    if (DEBUG) {
      recordConsumer = new RecordConsumerWrapper(recordConsumer);
    }
    read(recordConsumer);
    return newGroup;
  }

  public void read(RecordConsumer recordConsumer) {
    GroupColumnIO[] currentNodePath = new GroupColumnIO[16];
    int currentLevel = 0;
    currentNodePath[0] = root;
    int currentCol = 0;
    do {
      ColumnReader columnReader = columns[currentCol];
      PrimitiveColumnIO primitiveColumnIO = leaves[currentCol];
      int d = columnReader.getCurrentDefinitionLevel();
      int r = columnReader.getCurrentRepetitionLevel();
      if (d>=primitiveColumnIO.getDefinitionLevel()) {
        // not null
        if (DEBUG) log(Arrays.toString(primitiveColumnIO.getFieldPath())+" r:"+r+" d:"+d+" "+primitiveColumnIO.getType().asPrimitiveType().getPrimitive().toString(columnReader));
        for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1); ++currentLevel) {
          String field = primitiveColumnIO.getFieldPath()[currentLevel];
          currentNodePath[currentLevel + 1] = (GroupColumnIO)currentNodePath[currentLevel].getChild(field);
          if (DEBUG) log(field + " = new Group()");
          recordConsumer.startField(field);
          recordConsumer.startGroup();
        }
        String field = primitiveColumnIO.getFieldPath()[currentLevel];
        if (DEBUG) log(field + " = "+primitiveColumnIO.getType().asPrimitiveType().getPrimitive().toString(columnReader));
        recordConsumer.startField(field);

        primitiveColumnIO.getType().asPrimitiveType().getPrimitive().addValueToRecordConsumer(
            recordConsumer,
            columnReader);
        recordConsumer.endField(field);
      } else {
        if (DEBUG) log(Arrays.toString(primitiveColumnIO.getFieldPath())+" r:"+r+" d:"+d+" NULL "+currentLevel);
        for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1)
            && d > currentNodePath[currentLevel].getDefinitionLevel(); ++currentLevel) {
          String field = primitiveColumnIO.getFieldPath()[currentLevel];
          currentNodePath[currentLevel + 1] = (GroupColumnIO)currentNodePath[currentLevel].getChild(field);
          if (DEBUG) log(field + " = new Group()");
          recordConsumer.startField(field);
          recordConsumer.startGroup();
        }
      }
      columnReader.consume();
      int nextR = columnReader.isFullyConsumed() ? 0 : columnReader.getCurrentRepetitionLevel();
      int nextCol = nextReader[currentCol][nextR];
      // level to go to close current records
      int next = nextCol < leaves.length ? nextLevel[currentCol][nextR] : 0;
      for (; currentLevel > next; currentLevel--) {
        recordConsumer.endGroup();
        recordConsumer.endField(primitiveColumnIO.getFieldPath()[currentLevel-1]);
      }
      currentCol = nextCol;
    } while (currentCol < leaves.length);
  }

  private static void log(String string) {
    System.out.println(string);
  }

  int getNextReader(int current, int nextRepetitionLevel) {
    return nextReader[current][nextRepetitionLevel];
  }

  private int getCommonParentLevel(String[] previous, String[] next) {
    int i = 0;
    while (i < previous.length && i < next.length && previous[i].equals(next[i])) {
      ++i;
    }
    return i;
  }

}
