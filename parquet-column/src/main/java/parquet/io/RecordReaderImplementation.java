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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import parquet.Log;
import parquet.column.ColumnReader;
import parquet.column.mem.MemColumnReadStore;
import parquet.io.convert.Converter;
import parquet.io.convert.GroupConverter;
import parquet.io.convert.PrimitiveConverter;
import parquet.io.convert.RecordConverter;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;


/**
 * used to read reassembled records
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized record
 */
public class RecordReaderImplementation<T> extends RecordReader<T> {

  private static final Log LOG = Log.getLog(RecordReaderImplementation.class);
  private static final boolean DEBUG = Log.DEBUG;

  public static class Case {

    private int id;
    private final int startLevel;
    private final int depth;
    private final int nextLevel;
    private final boolean goingUp;
    private final boolean goingDown;
    private final int nextState;
    private final boolean defined;

    public Case(int startLevel, int depth, int nextLevel, int nextState, boolean defined) {
      this.startLevel = startLevel;
      this.depth = depth;
      this.nextLevel = nextLevel;
      this.nextState = nextState;
      this.defined = defined;
      // means going up the tree (towards the leaves) of the record
      // true if we need to open up groups in this case
      goingUp = startLevel <= depth;
      // means going down the tree (towards the root) of the record
      // true if we need to close groups in this case
      goingDown = depth + 1 > nextLevel;
    }

    public void setID(int id) {
      this.id = id;
    }

    @Override
    // this implementation is buggy but the simpler one bellow has duplicates.
    // it still works but generates more code than necessary
    // a middle ground is necessary
//    public int hashCode() {
//      int hashCode = 0;
//      if (goingUp) {
//        hashCode += 1 * (1 + startLevel) + 2 * (1 + depth);
//      }
//      if (goingDown) {
//        hashCode += 3 * (1 + depth) + 5 * (1 + nextLevel);
//      }
//      return hashCode;
//    }

    public int hashCode() {
      int hashCode = 17;
      hashCode += 31 * startLevel;
      hashCode += 31 * depth;
      hashCode += 31 * nextLevel;
      hashCode += 31 * nextState;
      hashCode += 31 * (defined ? 0 : 1);
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Case) {
        return equals((Case)obj);
      }
      return false;
    };

    // see comment for hashCode above
//    public boolean equals(Case other) {
//      if (goingUp && !other.goingUp || !goingUp && other.goingUp) {
//        return false;
//      }
//      if (goingUp && other.goingUp && (startLevel != other.startLevel || depth != other.depth)) {
//        return false;
//      }
//      if (goingDown && !other.goingDown || !goingDown && other.goingDown) {
//        return false;
//      }
//      if (goingDown && other.goingDown && (depth != other.depth || nextLevel != other.nextLevel)) {
//        return false;
//      }
//      return true;
//    }

    public boolean equals(Case other) {
      return startLevel == other.startLevel
          && depth == other.depth
          && nextLevel == other.nextLevel
          && nextState == other.nextState
          && ((defined && other.defined) || (!defined && !other.defined));
    }

    public int getID() {
      return id;
    }

    public int getStartLevel() {
      return startLevel;
    }

    public int getDepth() {
      return depth;
    }
    public int getNextLevel() {
      return nextLevel;
    }

    public int getNextState() {
      return nextState;
    }

    public boolean isGoingUp() {
      return goingUp;
    }

    public boolean isGoingDown() {
      return goingDown;
    }

    public boolean isDefined() {
      return defined;
    }

    @Override
    public String toString() {
      return "Case " + startLevel + " -> " + depth + " -> " + nextLevel + "; goto sate_"+getNextState();
    }

  }

  public static class State {

    public final int id;
    public final PrimitiveColumnIO primitiveColumnIO;
    public final int maxDefinitionLevel;
    public final int maxRepetitionLevel;
    public final PrimitiveTypeName primitive;
    public final ColumnReader column;
    public final String[] fieldPath; // indexed by currentLevel
    public final int[] indexFieldPath; // indexed by currentLevel
    public final GroupConverter[] groupConverterPath;
    public final PrimitiveConverter primitiveConverter;
    public final String primitiveField;
    public final int primitiveFieldIndex;
    public final int[] nextLevel; //indexed by next r

    private int[] definitionLevelToDepth; // indexed by current d
    private State[] nextState; // indexed by next r
    private Case[][][] caseLookup;
    private List<Case> definedCases;
    private List<Case> undefinedCases;

    private State(int id, PrimitiveColumnIO primitiveColumnIO, ColumnReader column, int[] nextLevel, GroupConverter[] groupConverterPath, PrimitiveConverter primitiveConverter) {
      this.id = id;
      this.primitiveColumnIO = primitiveColumnIO;
      this.maxDefinitionLevel = primitiveColumnIO.getDefinitionLevel();
      this.maxRepetitionLevel = primitiveColumnIO.getRepetitionLevel();
      this.column = column;
      this.nextLevel = nextLevel;
      this.groupConverterPath = groupConverterPath;
      this.primitiveConverter = primitiveConverter;
      this.primitive = primitiveColumnIO.getType().asPrimitiveType().getPrimitiveTypeName();
      this.fieldPath = primitiveColumnIO.getFieldPath();
      this.primitiveField = fieldPath[fieldPath.length - 1];
      this.indexFieldPath = primitiveColumnIO.getIndexFieldPath();
      this.primitiveFieldIndex = indexFieldPath[indexFieldPath.length - 1];
    }

    public int getDepth(int definitionLevel) {
      return definitionLevelToDepth[definitionLevel];
    }

    public List<Case> getDefinedCases() {
      return definedCases;
    }

    public List<Case> getUndefinedCases() {
      return undefinedCases;
    }

    public Case getCase(int currentLevel, int d, int nextR) {
      return caseLookup[currentLevel][d][nextR];
    }
  }

  private final GroupConverter recordConsumer;
  private final RecordConverter<T> recordMaterializer;

  private State[] states;

  /**
   *
   * @param root the root of the schema
   * @param leaves the leaves of the schema
   * @param validating
   * @param columns2
   */
  public RecordReaderImplementation(MessageColumnIO root, RecordConverter<T> recordMaterializer, boolean validating, MemColumnReadStore columnStore) {
    this.recordMaterializer = recordMaterializer;
    this.recordConsumer = recordMaterializer.getRootConverter(); // TODO: validator(wrap(recordMaterializer), validating, root.getType());
    PrimitiveColumnIO[] leaves = root.getLeaves().toArray(new PrimitiveColumnIO[root.getLeaves().size()]);
    ColumnReader[] columns = new ColumnReader[leaves.length];
    int[][] nextReader = new int[leaves.length][];
    int[][] nextLevel = new int[leaves.length][];
    GroupConverter[][] groupConverterPaths = new GroupConverter[leaves.length][];
    PrimitiveConverter[] primitiveConverters = new PrimitiveConverter[leaves.length];
    int[] firsts  = new int[256]; // "256 levels of nesting ought to be enough for anybody"
    // build the automaton
    for (int i = 0; i < leaves.length; i++) {
      PrimitiveColumnIO primitiveColumnIO = leaves[i];
      final int[] indexFieldPath = primitiveColumnIO.getIndexFieldPath();
      groupConverterPaths[i] = new GroupConverter[indexFieldPath.length - 1];
      GroupConverter current = this.recordConsumer;
      for (int j = 0; j < indexFieldPath.length - 1; j++) {
        current = current.getConverter(indexFieldPath[j]).asGroupConverter();
        groupConverterPaths[i][j] = current;
      }
      primitiveConverters[i] = current.getConverter(indexFieldPath[indexFieldPath.length - 1]).asPrimitiveConverter();
      columns[i] = columnStore.getColumnReader(primitiveColumnIO.getColumnDescriptor());
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
          throw new ParquetEncodingException(Arrays.toString(leaves[i].getFieldPath())+" -("+r+")-> "+nextLevel[i][r]);
        }
        nextReader[i][r] = next;
      }
    }
    states = new State[leaves.length];
    for (int i = 0; i < leaves.length; i++) {
      states[i] = new State(i, leaves[i], columns[i], nextLevel[i], groupConverterPaths[i], primitiveConverters[i]);

      int[] definitionLevelToDepth = new int[states[i].primitiveColumnIO.getDefinitionLevel() + 1];
      int depth = 0;
      // for each possible definition level, determine the depth at which to create groups
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
    for (int i = 0; i < states.length; i++) {
      State state = states[i];
      final Map<Case, Case> definedCases = new HashMap<Case, Case>();
      final Map<Case, Case> undefinedCases = new HashMap<Case, Case>();
      Case[][][] caseLookup = new Case[state.fieldPath.length][][];
      for (int currentLevel = 0; currentLevel < state.fieldPath.length; ++ currentLevel) {
        caseLookup[currentLevel] = new Case[state.maxDefinitionLevel+1][];
        for (int d = 0; d <= state.maxDefinitionLevel; ++ d) {
          caseLookup[currentLevel][d] = new Case[state.maxRepetitionLevel+1];
          for (int nextR = 0; nextR <= state.maxRepetitionLevel; ++ nextR) {
            int caseStartLevel = currentLevel;
            int caseDepth = Math.max(state.getDepth(d), caseStartLevel - 1);
            int caseNextLevel = Math.min(state.nextLevel[nextR], caseDepth + 1);
            Case currentCase = new Case(caseStartLevel, caseDepth, caseNextLevel, getNextReader(state.id, nextR), d == state.maxDefinitionLevel);
            Map<Case, Case> cases = currentCase.isDefined() ? definedCases : undefinedCases;
            if (!cases.containsKey(currentCase)) {
              currentCase.setID(cases.size());
              cases.put(currentCase, currentCase);
            } else {
              currentCase = cases.get(currentCase);
            }
            caseLookup[currentLevel][d][nextR] = currentCase;
          }
        }
      }
      state.caseLookup = caseLookup;
      state.definedCases = new ArrayList<Case>(definedCases.values());
      state.undefinedCases = new ArrayList<Case>(undefinedCases.values());
      Comparator<Case> caseComparator = new Comparator<Case>() {
        @Override
        public int compare(Case o1, Case o2) {
          return o1.id - o2.id;
        }
      };
      Collections.sort(state.definedCases, caseComparator);
      Collections.sort(state.undefinedCases, caseComparator);
    }
  }

  private RecordConsumer validator(RecordConsumer recordConsumer, boolean validating, MessageType schema) {
    return validating ? new ValidatingRecordConsumer(recordConsumer, schema) : recordConsumer;
  }

  private RecordConsumer wrap(RecordConsumer recordConsumer) {
    if (Log.DEBUG) {
      return new RecordConsumerLoggingWrapper(recordConsumer);
    }
    return recordConsumer;
  }

  /**
   * @see parquet.io.RecordReader#read()
   */
  @Override
  public T read() {
    int currentLevel = 0;
    State currentState = states[0];
    recordConsumer.start();
    do {
      ColumnReader columnReader = currentState.column;
      int d = columnReader.getCurrentDefinitionLevel();
      // creating needed nested groups until the current field (opening tags)
      int depth = currentState.definitionLevelToDepth[d];
      for (; currentLevel <= depth; ++currentLevel) {
        currentState.groupConverterPath[currentLevel].start();
      }
      // currentLevel = depth + 1 at this point
      // set the current value
      if (d >= currentState.maxDefinitionLevel) {
        // not null
        currentState.primitive.addValueToPrimitiveConverter(currentState.primitiveConverter, columnReader);
      }
      columnReader.consume();

      int nextR = currentState.maxRepetitionLevel == 0 ? 0 : columnReader.getCurrentRepetitionLevel();
      // level to go to close current groups
      int next = currentState.nextLevel[nextR];
      for (; currentLevel > next; currentLevel--) {
        currentState.groupConverterPath[currentLevel - 1].end();
      }

      currentState = currentState.nextState[nextR];
    } while (currentState != null);
    recordConsumer.end();
    return recordMaterializer.getCurrentRecord();
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
    while (i < Math.min(previous.length, next.length) && previous[i].equals(next[i])) {
      ++i;
    }
    return i;
  }

  protected int getStateCount() {
    return states.length;
  }

  protected State getState(int i) {
    return states[i];
  }

  protected RecordConverter<T> getMaterializer() {
    return recordMaterializer;
  }

  protected Converter getRecordConsumer() {
    return recordConsumer;
  }

}
