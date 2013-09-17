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

import java.util.Arrays;
import java.util.List;

import parquet.Log;
import parquet.column.ColumnWriteStore;
import parquet.column.ColumnWriter;
import parquet.column.impl.ColumnReadStoreImpl;
import parquet.column.page.PageReadStore;
import parquet.filter.UnboundRecordFilter;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.io.api.RecordMaterializer;
import parquet.schema.MessageType;

/**
 * Message level of the IO structure
 *
 *
 * @author Julien Le Dem
 *
 */
public class MessageColumnIO extends GroupColumnIO {
  private static final Log logger = Log.getLog(MessageColumnIO.class);

  private static final boolean DEBUG = Log.DEBUG;

  private List<PrimitiveColumnIO> leaves;

  private final boolean validating;

  MessageColumnIO(MessageType messageType, boolean validating) {
    super(messageType, null, 0);
    this.validating = validating;
  }

  public List<String[]> getColumnNames() {
    return super.getColumnNames();
  }

  public <T> RecordReader<T> getRecordReader(PageReadStore columns, RecordMaterializer<T> recordMaterializer) {
    if (leaves.size() > 0) {
      return new RecordReaderImplementation<T>(
        this,
        recordMaterializer,
        validating,
        new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType())
      );
    } else {
      return new EmptyRecordReader<T>(recordMaterializer);
    }
  }

  public <T> RecordReader<T> getRecordReader(PageReadStore columns, RecordMaterializer<T> recordMaterializer,
                                             UnboundRecordFilter unboundFilter) {

    return (unboundFilter == null)
      ? getRecordReader(columns, recordMaterializer)
      : new FilteredRecordReader<T>(
        this,
        recordMaterializer,
        validating,
        new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType()),
        unboundFilter,
        columns.getRowCount()
    );
  }

  private class MessageColumnIORecordConsumer extends RecordConsumer {
    private ColumnIO currentColumnIO;
    private int currentLevel = 0;
    private final int[] currentIndex;
    private final int[] r;
    private final ColumnWriter[] columnWriter;
    private boolean emptyField = true;

    public MessageColumnIORecordConsumer(ColumnWriteStore columns) {
      int maxDepth = 0;
      this.columnWriter = new ColumnWriter[MessageColumnIO.this.getLeaves().size()];
      for (PrimitiveColumnIO primitiveColumnIO : MessageColumnIO.this.getLeaves()) {
        maxDepth = Math.max(maxDepth, primitiveColumnIO.getFieldPath().length);
        columnWriter[primitiveColumnIO.getId()] = columns.getColumnWriter(primitiveColumnIO.getColumnDescriptor());
      }
      currentIndex = new int[maxDepth];
      r = new int[maxDepth];
    }

    public void printState() {
      log(currentLevel + ", " + currentIndex[currentLevel] + ": " + Arrays.toString(currentColumnIO.getFieldPath()) + " r:" + r[currentLevel]);
      if (r[currentLevel] > currentColumnIO.getRepetitionLevel()) {
        // sanity check
        throw new InvalidRecordException(r[currentLevel] + "(r) > " + currentColumnIO.getRepetitionLevel() + " ( schema r)");
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
      writeNullForMissingFields(((GroupColumnIO)currentColumnIO).getChildrenCount() - 1);
      if (DEBUG) log("< MESSAGE END >");
      if (DEBUG) printState();
    }

    @Override
    public void startField(String field, int index) {
      try {
        if (DEBUG) log("startField(" + field + ", " + index + ")");
        writeNullForMissingFields(index - 1);
        currentColumnIO = ((GroupColumnIO)currentColumnIO).getChild(index);
        currentIndex[currentLevel] = index;
        emptyField = true;
        if (DEBUG) printState();
      } catch (RuntimeException e) {
        throw new ParquetEncodingException("error starting field " + field + " at " + index, e);
      }
    }

    @Override
    public void endField(String field, int index) {
      if (DEBUG) log("endField(" + field + ", " + index + ")");
      currentColumnIO = currentColumnIO.getParent();
      if (emptyField) {
        throw new ParquetEncodingException("empty fields are illegal, the field should be ommited completely instead");
      }
      currentIndex[currentLevel] = index + 1;
      r[currentLevel] = currentLevel == 0 ? 0 : r[currentLevel - 1];
      if (DEBUG) printState();
    }

    private void writeNullForMissingFields(final int to) {
      final int from = currentIndex[currentLevel];
      for (;currentIndex[currentLevel]<=to; ++currentIndex[currentLevel]) {
        try {
          ColumnIO undefinedField = ((GroupColumnIO)currentColumnIO).getChild(currentIndex[currentLevel]);
          int d = currentColumnIO.getDefinitionLevel();
          if (DEBUG) log(Arrays.toString(undefinedField.getFieldPath()) + ".writeNull(" + r[currentLevel] + "," + d + ")");
          writeNull(undefinedField, r[currentLevel], d);
        } catch (RuntimeException e) {
          throw new ParquetEncodingException("error while writing nulls from " + from + " to " + to + ". current index: " + currentIndex[currentLevel], e);
        }
      }
    }

    private void writeNull(ColumnIO undefinedField, int r, int d) {
      if (undefinedField.getType().isPrimitive()) {
        columnWriter[((PrimitiveColumnIO)undefinedField).getId()].writeNull(r, d);
      } else {
        GroupColumnIO groupColumnIO = (GroupColumnIO)undefinedField;
        int childrenCount = groupColumnIO.getChildrenCount();
        for (int i = 0; i < childrenCount; i++) {
          writeNull(groupColumnIO.getChild(i), r, d);
        }
      }
    }

    private void setRepetitionLevel() {
      r[currentLevel] = currentColumnIO.getRepetitionLevel();
      if (DEBUG) log("r: " + r[currentLevel]);
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
      emptyField = false;
      int lastIndex = ((GroupColumnIO)currentColumnIO).getChildrenCount() - 1;
      writeNullForMissingFields(lastIndex);
      -- currentLevel;

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    private ColumnWriter getColumnWriter() {
      return columnWriter[((PrimitiveColumnIO)currentColumnIO).getId()];
    }

    @Override
    public void addInteger(int value) {
      if (DEBUG) log("addInt(" + value + ")");
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addLong(long value) {
      if (DEBUG) log("addLong(" + value + ")");
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addBoolean(boolean value) {
      if (DEBUG) log("addBoolean(" + value + ")");
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addBinary(Binary value) {
      if (DEBUG) log("addBinary(" + value.length() + " bytes)");
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }
    
    @Override
    public void addFixedBinary(Binary value) {
      if (DEBUG) log("addFixedBinary(" + value.length() + " bytes)");
      emptyField = false;
      getColumnWriter().writeFixed(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addFloat(float value) {
      if (DEBUG) log("addFloat(" + value + ")");
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

    @Override
    public void addDouble(double value) {
      if (DEBUG) log("addDouble(" + value + ")");
      emptyField = false;
      getColumnWriter().write(value, r[currentLevel], currentColumnIO.getDefinitionLevel());

      setRepetitionLevel();
      if (DEBUG) printState();
    }

  }

  public RecordConsumer getRecordWriter(ColumnWriteStore columns) {
    RecordConsumer recordWriter = new MessageColumnIORecordConsumer(columns);
    if (DEBUG) recordWriter = new RecordConsumerLoggingWrapper(recordWriter);
    return validating ? new ValidatingRecordConsumer(recordWriter, getType()) : recordWriter;
  }

  void setLevels() {
    setLevels(0, 0, new String[0], new int[0], Arrays.<ColumnIO>asList(this), Arrays.<ColumnIO>asList(this));
  }

  void setLeaves(List<PrimitiveColumnIO> leaves) {
    this.leaves = leaves;
  }

  public List<PrimitiveColumnIO> getLeaves() {
    return this.leaves;
  }

  @Override
  public MessageType getType() {
    return (MessageType)super.getType();
  }
}
