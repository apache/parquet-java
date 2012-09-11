package redelm.io;

import java.util.Arrays;
import java.util.List;

import redelm.Log;
import redelm.column.ColumnReader;
import redelm.data.GroupFactory;
import redelm.schema.PrimitiveType.Primitive;

public class RecordReader {

  static final boolean DEBUG = Log.DEBUG;

  private final PrimitiveColumnIO[] leaves;
  private final ColumnReader[] columns;
  private final int[][] nextReader;
  private final MessageColumnIO root;
  private final int[][] nextLevel;

  public RecordReader(MessageColumnIO root, List<PrimitiveColumnIO> leaves) {
    this.root = root;
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
        } else if (primitiveColumnIO.isLast(r)) { // reached the end of this level => close the repetition level
          ColumnIO parent = primitiveColumnIO.getParent(r);
          nextLevel[i][r] = parent.getFieldPath().length - 1;
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

  public void read(RecordConsumer recordConsumer) {
    GroupColumnIO[] currentNodePath = new GroupColumnIO[16];
    int currentLevel = 0;
    currentNodePath[0] = root;
    int currentCol = 0;
    startMessage(recordConsumer);
    do {
      ColumnReader columnReader = columns[currentCol];
      PrimitiveColumnIO primitiveColumnIO = leaves[currentCol];
      int d = columnReader.getCurrentDefinitionLevel();
      // creating needed nested groups until the current field (opening tags)
      for (; currentLevel < (primitiveColumnIO.getFieldPath().length - 1)
          && d > currentNodePath[currentLevel].getDefinitionLevel(); ++currentLevel) {
        startGroup(recordConsumer, currentNodePath, currentLevel, primitiveColumnIO);
      }
      // set the current value
      if (d >= primitiveColumnIO.getDefinitionLevel()) {
        // not null
        String field = primitiveColumnIO.getFieldPath()[currentLevel];
        int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel];
        if (DEBUG) log(field+"(" + currentLevel + ") = "+primitiveColumnIO.getType().asPrimitiveType().getPrimitive().toString(columnReader));
        addPrimitive(recordConsumer, columnReader, primitiveColumnIO.getType().asPrimitiveType().getPrimitive(), field, fieldIndex);
      }
      columnReader.consume();
      int nextR = columnReader.getCurrentRepetitionLevel();
      int nextCol = nextReader[currentCol][nextR];

      // level to go to close current groups
      int next = nextLevel[currentCol][nextR];
      for (; currentLevel > next; currentLevel--) {
        String field = primitiveColumnIO.getFieldPath()[currentLevel-1];
        int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel-1];
        endGroup(recordConsumer, field, fieldIndex);
      }
      currentCol = nextCol;
    } while (currentCol < leaves.length);
    endMessage(recordConsumer);
  }

  private void startMessage(RecordConsumer recordConsumer) {
    // reset state
    endField = null;
    recordConsumer.startMessage();
  }

  private void endMessage(RecordConsumer recordConsumer) {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    recordConsumer.endMessage();
  }

  private void addPrimitive(RecordConsumer recordConsumer, ColumnReader columnReader, Primitive primitive, String field, int index) {
    startField(recordConsumer, field, index);
    primitive.addValueToRecordConsumer(recordConsumer, columnReader);
    endField(recordConsumer, field, index);
  }

  private String endField;
  private int endIndex;

  private void endField(RecordConsumer recordConsumer, String field, int index) {
    if (endField != null) {
      recordConsumer.endField(endField, endIndex);
    }
    endField = field;
    endIndex = index;
  }

  private void startField(RecordConsumer recordConsumer, String field, int index) {
    if (endField != null && index == endIndex) {
      // skip the close/open tag
      endField = null;
    } else {
      if (endField != null) {
        // close the previous field
        recordConsumer.endField(endField, endIndex);
        endField = null;
      }
      recordConsumer.startField(field, index);
    }
  }

  private void endGroup(RecordConsumer recordConsumer, String field, int index) {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    recordConsumer.endGroup();
    endField(recordConsumer, field, index);
  }

  private void startGroup(RecordConsumer recordConsumer,
      GroupColumnIO[] currentNodePath, int currentLevel,
      PrimitiveColumnIO primitiveColumnIO) {
    String field = primitiveColumnIO.getFieldPath()[currentLevel];
    int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel];
    currentNodePath[currentLevel + 1] = (GroupColumnIO)currentNodePath[currentLevel].getChild(fieldIndex);
    if (DEBUG) log(field + "(" + currentLevel + ") = new Group()");
    startField(recordConsumer, field, fieldIndex);
    recordConsumer.startGroup();
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
