package redelm.io;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import redelm.Log;
import redelm.column.ColumnReader;
import redelm.data.Group;
import redelm.data.GroupFactory;

public class RecordReader {

  private static final boolean DEBUG = Log.DEBUG;

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
        // remember which is the first for this level
        if (primitiveColumnIO.isFirst(r)) {
          firsts[r] = i;
        }
        int next;
        // figure out automaton transition
        if (r == 0) { // 0 always means jump to the next (the last one being a special case)
          next = i + 1;
        } else if (primitiveColumnIO.isLast(r)) { // when we are at the last of the current repetition level we jump back to the first
          next = firsts[r];
        } else { // otherwise we just go back to the next.
          next = i + 1;
        }
        // figure out which level down the tree we need to go back
        if (next == this.leaves.length) { // reached the end of the record => close all levels
          nextLevel[i][r] = 0;
//        } else if (primitiveColumnIO.isLast(r) && primitiveColumnIO.isFirst(r)) {
//          // if first and last => this is the rep level.
//          nextLevel[i][r] = primitiveColumnIO.getFieldPath().length - 1;
////          ColumnIO parent = primitiveColumnIO.getParent(r);
////          nextLevel[i][r] = parent.getFieldPath().length - 1;
//          System.out.println("1%%%%% "+Arrays.toString(this.leaves[i].getFieldPath())+"["+r+"] "+nextLevel[i][r]);
        } else if (primitiveColumnIO.isLast(r)) { // reached the end of this level => close the repetition level
          ColumnIO parent = primitiveColumnIO.getParent(r);
          nextLevel[i][r] = parent.getFieldPath().length - 1;
          if (DEBUG) log("2%%%%% "+Arrays.toString(this.leaves[i].getFieldPath())+"["+r+"] "+nextLevel[i][r]);
        } else { // otherwise close until the next common parent
          nextLevel[i][r] = getCommonParentLevel(
              primitiveColumnIO.getFieldPath(),
              this.leaves[next].getFieldPath());
        }
        // sanity check: that would be a bug
        if (nextLevel[i][r] > this.leaves[i].getFieldPath().length-1) {
          throw new RuntimeException(Arrays.toString(this.leaves[i].getFieldPath())+" -("+r+")-> "+nextLevel[i][r]);
        }
        this.nextReader[i][r] = next;
      }
    }
  }

  private static class SimpleGroupRecordConsumer extends RecordConsumer {

    private final Deque<Group> groups = new ArrayDeque<Group>();
    private final Deque<Integer> fields = new ArrayDeque<Integer>();
    private final GroupFactory groupFactory;
    private final Collection<Group> result;

    public SimpleGroupRecordConsumer(GroupFactory groupFactory, Collection<Group> result) {
      this.groupFactory = groupFactory;
      this.result = result;
    }

    @Override
    public void startField(String field, int index) {
      fields.push(index);
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
    public void endField(String field, int index) {
      if (DEBUG) if (!fields.peek().equals(index)) throw new IllegalStateException("opening "+fields.peek()+" but closing "+index+" ("+field+")");
      fields.pop();
    }

    @Override
    public void startMessage() {
      groups.push(groupFactory.newGroup());
    }

    @Override
    public void endMessage() {
      if (DEBUG) if (groups.size() != 1) throw new IllegalStateException("end of message in the middle of a record "+fields);
      this.result.add(groups.pop());
    }

  }

  public Group read() {
    List<Group> result = new ArrayList<Group>();
    RecordConsumer recordConsumer = new SimpleGroupRecordConsumer(groupFactory, result);
    if (DEBUG) {
      recordConsumer = new RecordConsumerWrapper(recordConsumer);
    }
    read(recordConsumer);
    return result.get(0);
  }

  public void read(RecordConsumer recordConsumer) {
    GroupColumnIO[] currentNodePath = new GroupColumnIO[16];
    currentNodePath[0] = root;
    int currentCol = 0;
    int lowLevel = 0;
    int currentLevel = 0;
    PrimitiveColumnIO primitiveColumnIO = null;
    recordConsumer.startMessage();
    do {
      // closing the levels that need to be closed
      // except the first time when we start a new record fresh
      if (primitiveColumnIO != null) {
        if (DEBUG) log("<=== "+currentLevel+" to "+lowLevel);
        for (; currentLevel > lowLevel; --currentLevel) {
          String field = primitiveColumnIO.getFieldPath()[currentLevel-1];
          int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel-1];
          recordConsumer.endGroup();
          recordConsumer.endField(field, fieldIndex);
        }
        if (DEBUG) log("<=== done");
      }
      ColumnReader columnReader = columns[currentCol];
      primitiveColumnIO = leaves[currentCol];
      int d = columnReader.getCurrentDefinitionLevel();
      if (DEBUG) log(">=== "+currentLevel+" to "+(primitiveColumnIO.getFieldPath().length - 1));
      // creating needed nested groups until the current field (opening tags)
      for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1)
          && d > currentNodePath[currentLevel].getDefinitionLevel(); ++currentLevel) {
        String field1 = primitiveColumnIO.getFieldPath()[currentLevel];
        int fieldIndex1 = primitiveColumnIO.getIndexFieldPath()[currentLevel];
        currentNodePath[currentLevel + 1] = (GroupColumnIO)currentNodePath[currentLevel].getChild(fieldIndex1);
        if (DEBUG) log(field1 + "(" + currentLevel + ") = new Group()");
        recordConsumer.startField(field1, fieldIndex1);
        recordConsumer.startGroup();
      }
      if (DEBUG) log(">=== done");
      String field = primitiveColumnIO.getFieldPath()[currentLevel];
      int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel];
      // set the current value
      if (d == primitiveColumnIO.getDefinitionLevel()) {
        // not null
        recordConsumer.startField(field, fieldIndex);
        if (DEBUG) log(field+"(" + currentLevel + ") = "+primitiveColumnIO.getType().asPrimitiveType().getPrimitive().toString(columnReader));
        primitiveColumnIO.getType().asPrimitiveType().getPrimitive().addValueToRecordConsumer(recordConsumer, columnReader);
      }
      columnReader.consume();
      int nextR = columnReader.getCurrentRepetitionLevel();
      int nextCol = nextReader[currentCol][nextR];
      // level to go to close current groups
      int next = nextLevel[currentCol][nextR];
      if (d == primitiveColumnIO.getDefinitionLevel()) {
        recordConsumer.endField(field, fieldIndex);
      }
      lowLevel = next;
      currentCol = nextCol;
    } while (currentCol < leaves.length);
    if (DEBUG) log("<=== "+currentLevel+" to "+lowLevel);
    for (; currentLevel > 0; --currentLevel) {
      String field = primitiveColumnIO.getFieldPath()[currentLevel-1];
      int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel-1];
      recordConsumer.endGroup();
      recordConsumer.endField(field, fieldIndex);
    }
    if (DEBUG) log("<=== done");

    recordConsumer.endMessage();
  }

  private static void log(String string) {
    System.out.println(string);
  }

  int getNextReader(int current, int nextRepetitionLevel) {
    return nextReader[current][nextRepetitionLevel];
  }

  int getNextLevel(int current, int nextRepetitionLevel) {
    return nextLevel[current][nextRepetitionLevel];
  }

  private int getCommonParentLevel(String[] previous, String[] next) {
    int i = 0;
    while (i < previous.length && i < next.length && previous[i].equals(next[i])) {
      ++i;
    }
    return i;
  }

}
