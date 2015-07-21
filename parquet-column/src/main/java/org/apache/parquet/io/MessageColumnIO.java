/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.Log;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.FilterPredicateCompat;
import org.apache.parquet.filter2.compat.FilterCompat.NoOpFilter;
import org.apache.parquet.filter2.compat.FilterCompat.UnboundRecordFilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.recordlevel.FilteringRecordMaterializer;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicate;
import org.apache.parquet.filter2.recordlevel.IncrementallyUpdatedFilterPredicateBuilder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import static org.apache.parquet.Preconditions.checkNotNull;

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
  private final String createdBy;

  MessageColumnIO(MessageType messageType, boolean validating, String createdBy) {
    super(messageType, null, 0);
    this.validating = validating;
    this.createdBy = createdBy;
  }

  public List<String[]> getColumnNames() {
    return super.getColumnNames();
  }

  public <T> RecordReader<T> getRecordReader(PageReadStore columns,
                                             RecordMaterializer<T> recordMaterializer) {
    return getRecordReader(columns, recordMaterializer, FilterCompat.NOOP);
  }

  /**
   * @deprecated use {@link #getRecordReader(PageReadStore, RecordMaterializer, Filter)}
   */
  @Deprecated
  public <T> RecordReader<T> getRecordReader(PageReadStore columns,
                                             RecordMaterializer<T> recordMaterializer,
                                             UnboundRecordFilter filter) {
    return getRecordReader(columns, recordMaterializer, FilterCompat.get(filter));
  }

  public <T> RecordReader<T> getRecordReader(final PageReadStore columns,
                                             final RecordMaterializer<T> recordMaterializer,
                                             final Filter filter) {
    checkNotNull(columns, "columns");
    checkNotNull(recordMaterializer, "recordMaterializer");
    checkNotNull(filter, "filter");

    if (leaves.isEmpty()) {
      return new EmptyRecordReader<T>(recordMaterializer);
    }

    return filter.accept(new Visitor<RecordReader<T>>() {
      @Override
      public RecordReader<T> visit(FilterPredicateCompat filterPredicateCompat) {

        FilterPredicate predicate = filterPredicateCompat.getFilterPredicate();
        IncrementallyUpdatedFilterPredicateBuilder builder = new IncrementallyUpdatedFilterPredicateBuilder();
        IncrementallyUpdatedFilterPredicate streamingPredicate = builder.build(predicate);
        RecordMaterializer<T> filteringRecordMaterializer = new FilteringRecordMaterializer<T>(
            recordMaterializer,
            leaves,
            builder.getValueInspectorsByColumn(),
            streamingPredicate);

        return new RecordReaderImplementation<T>(
            MessageColumnIO.this,
            filteringRecordMaterializer,
            validating,
            new ColumnReadStoreImpl(columns, filteringRecordMaterializer.getRootConverter(), getType(), createdBy));
      }

      @Override
      public RecordReader<T> visit(UnboundRecordFilterCompat unboundRecordFilterCompat) {
        return new FilteredRecordReader<T>(
            MessageColumnIO.this,
            recordMaterializer,
            validating,
            new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType(), createdBy),
            unboundRecordFilterCompat.getUnboundRecordFilter(),
            columns.getRowCount()
        );
      }

      @Override
      public RecordReader<T> visit(NoOpFilter noOpFilter) {
        return new RecordReaderImplementation<T>(
            MessageColumnIO.this,
            recordMaterializer,
            validating,
            new ColumnReadStoreImpl(columns, recordMaterializer.getRootConverter(), getType(), createdBy));
      }
    });
  }

  private class MessageColumnIORecordConsumer extends RecordConsumer {
    private ColumnIO currentColumnIO;
    private int currentLevel = 0;

    private class FieldsMarker {
      private BitSet vistedIndexes = new BitSet();

      @Override
      public String toString() {
        return "VistedIndex{" +
                "vistedIndexes=" + vistedIndexes +
                '}';
      }

      public void reset(int fieldsCount) {
        this.vistedIndexes.clear(0, fieldsCount);
      }

      public void markWritten(int i) {
        vistedIndexes.set(i);
      }

      public boolean isWritten(int i) {
        return vistedIndexes.get(i);
      }
    }

    //track at each level of depth, which fields are written, so nulls can be inserted for the unwritten fields
    private final FieldsMarker[] fieldsWritten;
    private final int[] r;
    private final ColumnWriter[] columnWriter;
    /** maintain a map of a group and all the leaf nodes underneath it. It's used to optimize writing null for a group node
     * all the leaves can be called directly without traversing the sub tree of the group node */
    private Map<GroupColumnIO, List<ColumnWriter>>  groupToLeafWriter = new HashMap<GroupColumnIO, List<ColumnWriter>>();
    private final ColumnWriteStore columns;
    private boolean emptyField = true;

    private void buildGroupToLeafWriterMap(PrimitiveColumnIO primitive, ColumnWriter writer) {
      GroupColumnIO  parent = primitive.getParent();
      do {
        getLeafWriters(parent).add(writer);
      } while ((parent = parent.getParent()) != null);
    }

    private List<ColumnWriter> getLeafWriters(GroupColumnIO group) {
      if (!groupToLeafWriter.containsKey(group)) {
        groupToLeafWriter.put(group, new ArrayList<ColumnWriter>());
      }
      return groupToLeafWriter.get(group);
    }

    public MessageColumnIORecordConsumer(ColumnWriteStore columns) {
      this.columns = columns;
      int maxDepth = 0;
      this.columnWriter = new ColumnWriter[MessageColumnIO.this.getLeaves().size()];

      for (PrimitiveColumnIO primitiveColumnIO : MessageColumnIO.this.getLeaves()) {
        ColumnWriter w = columns.getColumnWriter(primitiveColumnIO.getColumnDescriptor());
        maxDepth = Math.max(maxDepth, primitiveColumnIO.getFieldPath().length);
        columnWriter[primitiveColumnIO.getId()] = w;
        buildGroupToLeafWriterMap(primitiveColumnIO, w);
      }

      fieldsWritten = new FieldsMarker[maxDepth];
      for (int i = 0; i < maxDepth; i++) {
        fieldsWritten[i] = new FieldsMarker();
      }
      r = new int[maxDepth];
    }

    public void printState() {
      log(currentLevel + ", " + fieldsWritten[currentLevel] + ": " + Arrays.toString(currentColumnIO.getFieldPath()) + " r:" + r[currentLevel]);
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
      int numberOfFieldsToVisit = ((GroupColumnIO)currentColumnIO).getChildrenCount();
      fieldsWritten[0].reset(numberOfFieldsToVisit);
      if (DEBUG) printState();
    }

    @Override
    public void endMessage() {
      writeNullForMissingFieldsAtCurrentLevel();
      columns.endRecord();
      if (DEBUG) log("< MESSAGE END >");
      if (DEBUG) printState();
    }

    @Override
    public void startField(String field, int index) {
      try {
        if (DEBUG) log("startField(" + field + ", " + index + ")");
        currentColumnIO = ((GroupColumnIO)currentColumnIO).getChild(index);
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
      fieldsWritten[currentLevel].markWritten(index);
      r[currentLevel] = currentLevel == 0 ? 0 : r[currentLevel - 1];
      if (DEBUG) printState();
    }

    private void writeNullForMissingFieldsAtCurrentLevel() {
      int currentFieldsCount = ((GroupColumnIO)currentColumnIO).getChildrenCount();
      for (int i = 0; i < currentFieldsCount; i++) {
        if (!fieldsWritten[currentLevel].isWritten(i)) {
          try {
            ColumnIO undefinedField = ((GroupColumnIO)currentColumnIO).getChild(i);
            int d = currentColumnIO.getDefinitionLevel();
            if (DEBUG)
              log(Arrays.toString(undefinedField.getFieldPath()) + ".writeNull(" + r[currentLevel] + "," + d + ")");
            writeNull(undefinedField, r[currentLevel], d);
          } catch (RuntimeException e) {
            throw new ParquetEncodingException("error while writing nulls for fields of indexes " + i + " . current index: " + fieldsWritten[currentLevel], e);
          }
        }
      }
    }

    private void writeNull(ColumnIO undefinedField, int r, int d) {
      if (undefinedField.getType().isPrimitive()) {
        columnWriter[((PrimitiveColumnIO)undefinedField).getId()].writeNull(r, d);
      } else {
        GroupColumnIO groupColumnIO = (GroupColumnIO)undefinedField;
        writeNullToLeaves(groupColumnIO, r, d);
      }
    }

    private void writeNullToLeaves(GroupColumnIO group, int r, int d) {
      for(ColumnWriter leafWriter: groupToLeafWriter.get(group)) {
        leafWriter.writeNull(r,d);
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

      int fieldsCount = ((GroupColumnIO)currentColumnIO).getChildrenCount();
      fieldsWritten[currentLevel].reset(fieldsCount);
      if (DEBUG) printState();
    }

    @Override
    public void endGroup() {
      if (DEBUG) log("endGroup()");
      emptyField = false;
      writeNullForMissingFieldsAtCurrentLevel();
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
