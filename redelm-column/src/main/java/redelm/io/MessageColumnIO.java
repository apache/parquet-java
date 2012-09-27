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
import java.util.List;

import redelm.Log;
import redelm.column.ColumnsStore;
import redelm.schema.MessageType;


public class MessageColumnIO extends GroupColumnIO {
  private static final Log logger = Log.getLog(MessageColumnIO.class);

  private static final boolean DEBUG = Log.DEBUG;

  private List<PrimitiveColumnIO> leaves;

  MessageColumnIO(MessageType messageType) {
    super(messageType, null);
  }

  public List<String[]> getColumnNames() {
    return super.getColumnNames();
  }

  public RecordReader getRecordReader() {
    return new RecordReader(this, leaves);
  }

  public RecordConsumer getRecordWriter() {
    return new RecordConsumer() {

      ColumnIO currentColumnIO;
      int currentLevel = 0;
      int[] currentIndex = new int[16];
      int[] r = new int[16];

      public void printState() {
        log(currentLevel+", "+currentIndex[currentLevel]+": "+Arrays.toString(currentColumnIO.getType().getFieldPath())+" r:"+r[currentLevel]);
        if (r[currentLevel] > currentColumnIO.getRepetitionLevel()) {
          // sanity check
          throw new RuntimeException(r[currentLevel]+"(r) > "+currentColumnIO.getRepetitionLevel()+" ( schema r)");
        }
      }

      private void log(Object m) {
        String indent = "";
        for (int i = 0; i<currentLevel; ++i) {
          indent += "  ";
        }
        logger.debug(indent + m);
      }

      @Override
      public void startMessage() {
        if (DEBUG) log("< MESSAGE START >");
        currentColumnIO = MessageColumnIO.this;
        r[0] = 0;
        currentIndex[0] = 0;
        if (DEBUG) printState();
      }

      @Override
      public void endMessage() {
        if (DEBUG) log("< MESSAGE END >");
        if (DEBUG) printState();
      }

      @Override
      public void startField(String field, int index) {
        if (DEBUG) log("startField("+field+", "+index+")");
        writeNullForMissingFields(index - 1);
        currentColumnIO = ((GroupColumnIO)currentColumnIO).getChild(index);
        currentIndex[currentLevel] = index;
        if (DEBUG) printState();
      }

      private void writeNullForMissingFields(int to) {
        for (;currentIndex[currentLevel]<=to; ++currentIndex[currentLevel]) {
          ColumnIO undefinedField = ((GroupColumnIO)currentColumnIO).getChild(currentIndex[currentLevel]);
          int d = currentColumnIO.getDefinitionLevel();
          if (DEBUG) log(Arrays.toString(undefinedField.getType().getFieldPath())+".writeNull("+r[currentLevel]+","+d+")");
          undefinedField.writeNull(r[currentLevel], d);
        }
      }

      private void setRepetitionLevel() {
        r[currentLevel] = currentColumnIO.getRepetitionLevel();
        if (DEBUG) log("r: "+r[currentLevel]);
      }

      @Override
      public void endField(String field, int index) {
        if (DEBUG) log("endField("+field+", "+index+")");
        currentColumnIO = currentColumnIO.getParent();

        currentIndex[currentLevel] = index + 1;

        r[currentLevel] = currentLevel == 0 ? 0 : r[currentLevel - 1];

        if (DEBUG) printState();
      }

      @Override
      public void startGroup() {
        if (DEBUG) log("startGroup()");

        ++ currentLevel;
        r[currentLevel] = r[currentLevel - 1];

        currentIndex[currentLevel] = 0;
        if (DEBUG) printState();
      }

      @Override
      public void endGroup() {
        if (DEBUG) log("endGroup()");
        int lastIndex = ((GroupColumnIO)currentColumnIO).getChildrenCount() -1;
        writeNullForMissingFields(lastIndex);

        -- currentLevel;

        setRepetitionLevel();
        if (DEBUG) printState();
      }

      @Override
      public void addInt(int value) {
        if (DEBUG) log("addInt("+value+")");

        ((PrimitiveColumnIO)currentColumnIO).getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

        setRepetitionLevel();
        if (DEBUG) printState();
      }

      @Override
      public void addString(String value) {
        if (DEBUG) log("addString("+value+")");

        ((PrimitiveColumnIO)currentColumnIO).getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

        setRepetitionLevel();
        if (DEBUG) printState();
      }

      @Override
      public void addBoolean(boolean value) {
        if (DEBUG) log("addBoolean("+value+")");
        ((PrimitiveColumnIO)currentColumnIO).getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

        setRepetitionLevel();
        if (DEBUG) printState();
      }

      @Override
      public void addBinary(byte[] value) {
        if (DEBUG) log("addBinary("+value+")");
        ((PrimitiveColumnIO)currentColumnIO).getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

        setRepetitionLevel();
        if (DEBUG) printState();
      }
      };
  }

  void setLevels(ColumnsStore columns) {
    setLevels(0, 0, new String[0], new int[0], Arrays.<ColumnIO>asList(this), Arrays.<ColumnIO>asList(this), columns);
  }

  void setLeaves(List<PrimitiveColumnIO> leaves) {
    this.leaves = leaves;
  }

  public List<PrimitiveColumnIO> getLeaves() {
    return this.leaves;
  }

}
