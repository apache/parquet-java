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

import java.util.Arrays;

import redelm.Log;
import redelm.column.ColumnReader;
import redelm.data.Group;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType.Primitive;

/**
 * used to read reassembled records
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
public class RecordReader<T> {

  private static final Log LOG = Log.getLog(RecordReader.class);
  private static final boolean DEBUG = Log.DEBUG;

  private static class State {

    private final int id;
    private final PrimitiveColumnIO primitiveColumnIO;
    private final Primitive primitive;
    private final ColumnReader column;
    private final String[] fieldPath;
    private final int[] indexFieldPath;
    private final int[] nextLevel;

    private int[] definitionLevelToDepth;
    private State[] nextState;

    private State(int id, PrimitiveColumnIO primitiveColumnIO, ColumnReader column, int[] nextLevel) {
      this.id = id;
      this.primitiveColumnIO = primitiveColumnIO;
      this.column = column;
      this.nextLevel = nextLevel;
      this.primitive = primitiveColumnIO.getType().asPrimitiveType().getPrimitive();
      this.fieldPath = primitiveColumnIO.getFieldPath();
      this.indexFieldPath = primitiveColumnIO.getIndexFieldPath();
    }

  }

  private final RecordConsumer recordConsumer;
  private final RecordMaterializer<T> recordMaterializer;

  private String endField;
  private int endIndex;
  private State[] states;

  /**
   *
   * @param root the root of the schema
   * @param leaves the leaves of the schema
   * @param validating
   */
  public RecordReader(MessageColumnIO root, RecordMaterializer<T> recordMaterializer, boolean validating) {
    this.recordMaterializer = recordMaterializer;
    this.recordConsumer = validator(wrap(recordMaterializer), validating, root.getType());
    PrimitiveColumnIO[] leaves = root.getLeaves().toArray(new PrimitiveColumnIO[root.getLeaves().size()]);
    ColumnReader[] columns = new ColumnReader[leaves.length];
    int[][] nextReader = new int[leaves.length][];
    int[][] nextLevel = new int[leaves.length][];
    int[] firsts  = new int[256]; // "256 levels of nesting ought to be enough for anybody"
    // build the automaton
    for (int i = 0; i < leaves.length; i++) {
      PrimitiveColumnIO primitiveColumnIO = leaves[i];
      columns[i] = primitiveColumnIO.getColumnReader();
      int repetitionLevel = primitiveColumnIO.getRepetitionLevel();
      nextReader[i] = new int[repetitionLevel+1];
      nextLevel[i] = new int[repetitionLevel+1];
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
        if (next == leaves.length) { // reached the end of the record => close all levels
          nextLevel[i][r] = 0;
        } else if (primitiveColumnIO.isLast(r)) { // reached the end of this level => close the repetition level
          ColumnIO parent = primitiveColumnIO.getParent(r);
          nextLevel[i][r] = parent.getFieldPath().length - 1;
        } else { // otherwise close until the next common parent
          nextLevel[i][r] = getCommonParentLevel(
              primitiveColumnIO.getFieldPath(),
              leaves[next].getFieldPath());
        }
        // sanity check: that would be a bug
        if (nextLevel[i][r] > leaves[i].getFieldPath().length-1) {
          throw new RuntimeException(Arrays.toString(leaves[i].getFieldPath())+" -("+r+")-> "+nextLevel[i][r]);
        }
        nextReader[i][r] = next;
      }
    }
    states = new State[leaves.length];
    for (int i = 0; i < leaves.length; i++) {
      states[i] = new State(i, leaves[i], columns[i], nextLevel[i]);

      int[] definitionLevelToDepth = new int[states[i].primitiveColumnIO.getDefinitionLevel() + 1];
      int depth = 0;
      for (int d = 0; d < definitionLevelToDepth.length; ++d) {
        while (depth < (states[i].fieldPath.length - 1)
          && d > states[i].primitiveColumnIO.getPath()[depth].getDefinitionLevel()) {
          ++ depth;
        }
        definitionLevelToDepth[d] = depth - 1;
      }
      states[i].definitionLevelToDepth = definitionLevelToDepth;

    }
    for (int i = 0; i < leaves.length; i++) {
      State state = states[i];
      int[] nextStateIds = nextReader[i];
      state.nextState = new State[nextStateIds.length];
      for (int j = 0; j < nextStateIds.length; j++) {
        state.nextState[j] = nextStateIds[j] == states.length ? null : states[nextStateIds[j]];
      }
    }
  }

  private RecordConsumer validator(RecordConsumer recordConsumer, boolean validating, MessageType schema) {
    return validating ? new ValidatingRecordConsumer<Group>(recordConsumer, schema) : recordConsumer;
  }

  private RecordConsumer wrap(RecordConsumer recordConsumer) {
    if (Log.DEBUG) {
      return new RecordConsumerWrapper(recordConsumer);
    }
    return recordConsumer;
  }

  /**
   * reads one record and writes it in the RecordConsumer
   * @param recordConsumer
   */
  public T read() {
    int currentLevel = 0;
    State currentState = states[0];
    startMessage();
    do {
      ColumnReader columnReader = currentState.column;
      PrimitiveColumnIO primitiveColumnIO = currentState.primitiveColumnIO;
      int d = columnReader.getCurrentDefinitionLevel();
      // creating needed nested groups until the current field (opening tags)
//      for (; currentLevel < (currentState.fieldPath.length - 1)
//          && d > primitiveColumnIO.getPath()[currentLevel].getDefinitionLevel(); ++currentLevel) {
//        startGroup(recordConsumer, currentLevel, primitiveColumnIO);
//      }
      int depth = currentState.definitionLevelToDepth[d];
      for (; currentLevel <= depth; ++currentLevel) {
        startGroup(recordConsumer, currentLevel, primitiveColumnIO);
      }
      // set the current value
      if (d >= primitiveColumnIO.getDefinitionLevel()) {
        // not null
        String field = currentState.fieldPath[currentLevel];
        int fieldIndex = currentState.indexFieldPath[currentLevel];
        if (DEBUG) log(field+"(" + currentLevel + ") = "+currentState.primitive.toString(columnReader));
        addPrimitive(columnReader, currentState.primitive, field, fieldIndex);
      }
      columnReader.consume();
      int nextR = columnReader.getCurrentRepetitionLevel();
      State nextState = currentState.nextState[nextR];

      // level to go to close current groups
      int next = currentState.nextLevel[nextR];
      for (; currentLevel > next; currentLevel--) {
        String field = currentState.fieldPath[currentLevel-1];
        int fieldIndex = currentState.indexFieldPath[currentLevel-1];
        endGroup(field, fieldIndex);
      }
      currentState = nextState;
    } while (currentState != null);
    endMessage();
    return recordMaterializer.getCurrentRecord();
  }

  private void startMessage() {
    // reset state
    endField = null;
    recordConsumer.startMessage();
  }

  private void endMessage() {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    recordConsumer.endMessage();
  }

  private void addPrimitive(ColumnReader columnReader, Primitive primitive, String field, int index) {
    startField(field, index);
    primitive.addValueToRecordConsumer(recordConsumer, columnReader);
    endField(field, index);
  }

  private void endField(String field, int index) {
    if (endField != null) {
      recordConsumer.endField(endField, endIndex);
    }
    endField = field;
    endIndex = index;
  }

  private void startField(String field, int index) {
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

  private void endGroup(String field, int index) {
    if (endField != null) {
      // close the previous field
      recordConsumer.endField(endField, endIndex);
      endField = null;
    }
    recordConsumer.endGroup();
    endField(field, index);
  }

  private void startGroup(RecordConsumer recordConsumer, int currentLevel,
      PrimitiveColumnIO primitiveColumnIO) {
    String field = primitiveColumnIO.getFieldPath()[currentLevel];
    int fieldIndex = primitiveColumnIO.getIndexFieldPath()[currentLevel];
    if (DEBUG) log(field + "(" + currentLevel + ") = new Group()");
    startField(field, fieldIndex);
    recordConsumer.startGroup();
  }

  private static void log(String string) {
    LOG.debug(string);
  }

  int getNextReader(int current, int nextRepetitionLevel) {
    State nextState = states[current].nextState[nextRepetitionLevel];
    return nextState == null ? states.length : nextState.id;
  }

  int getNextLevel(int current, int nextRepetitionLevel) {
    return states[current].nextLevel[nextRepetitionLevel];
  }

  private int getCommonParentLevel(String[] previous, String[] next) {
    int i = 0;
    while (i < previous.length && i < next.length && previous[i].equals(next[i])) {
      ++i;
    }
    return i;
  }

}
