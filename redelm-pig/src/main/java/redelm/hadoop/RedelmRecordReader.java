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
package redelm.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import redelm.column.mem.MemColumnsStore;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.io.RecordConsumer;
import redelm.parser.MessageTypeParser;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads the records from a block of a RedElm file
 *
 * @see RedelmInputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> type of the materialized records
 */
public class RedelmRecordReader<T> extends RecordReader<Void, T> {

  private ColumnIOFactory columnIOFactory = new ColumnIOFactory();
  private Map<String, ColumnMetaData> columnTypes = new HashMap<String, ColumnMetaData>();
  private T currentValue;
  private int total;
  private int current;
  private RedelmFileReader reader;
  private List<T> destination = new LinkedList<T>();
  private redelm.io.RecordReader recordReader;
  private MemColumnsStore columnsStore;
  private RecordConsumer recordConsumer;
  private FSDataInputStream f;
  private ReadSupport<T> readSupport;
  private MessageType requestedSchema;

  /**
   *
   * @param requestedSchema the requested schema (a subset of the original schema) for record projection
   */
  RedelmRecordReader(String requestedSchema) {
    this.requestedSchema = MessageTypeParser.parseMessageType(requestedSchema);
  }

  private void checkRead() throws IOException {
    if (columnsStore == null || columnsStore.isFullyConsumed()) {
      columnsStore = new MemColumnsStore(0);
      BlockData readColumns = reader.readColumns();
      if (readColumns == null) {
        return;
      }
      for (ColumnData columnData : readColumns.getColumns()) {
        ColumnMetaData columnMetaData = columnTypes.get(Arrays.toString(columnData.getPath()));
        columnsStore.setForRead(
            columnData.getPath(), columnMetaData.getType(),
            columnMetaData.getValueCount(),
            columnData.getRepetitionLevels(),
            columnData.getDefinitionLevels(),
            columnData.getData()
            );
      }
      MessageColumnIO columnIO = columnIOFactory.getColumnIO(requestedSchema, columnsStore);
      recordReader = columnIO.getRecordReader();
      recordConsumer = readSupport.newRecordConsumer(destination);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    f.close();
  }

  /**
   * always returns null
   */
  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T getCurrentValue() throws IOException,
  InterruptedException {
    return currentValue;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (float)current/total;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
    @SuppressWarnings("unchecked") // I know
    RedelmInputSplit<T> redelmInputSplit = (RedelmInputSplit<T>)inputSplit;
    this.readSupport = redelmInputSplit.getReadSupport();
    Path path = redelmInputSplit.getPath();
    f = fs.open(path);
    BlockMetaData block = redelmInputSplit.getBlock();
    List<String[]> columns = new ArrayList<String[]>();
    for (ColumnMetaData columnMetaData : block.getColumns()) {
      columnTypes.put(Arrays.toString(columnMetaData.getPath()), columnMetaData);
      if (contains(requestedSchema, columnMetaData.getPath())) {
        columns.add(columnMetaData.getPath());
      }
    }
    reader = new RedelmFileReader(f, Arrays.asList(block), columns, redelmInputSplit.getFileMetaData().getCodecClassName());
    total = block.getRecordCount();
  }

  private boolean contains(GroupType requestedSchema, String[] path) {
    return contains(requestedSchema, path, 0);
  }

  private boolean contains(GroupType group, String[] path, int index) {
    if (index == path.length) {
      return false;
    }
    if (group.containsField(path[index])) {
      Type type = group.getType(path[index]);
      if (type.isPrimitive()) {
        return index + 1 == path.length;
      } else {
        return contains(type.asGroupType(), path, index + 1);
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    checkRead();
    if (destination.size() == 0 && current<total) {
      recordReader.read(recordConsumer);
    }
    if (destination.size() == 0) {
      currentValue = null;
      return false;
    }
    current ++;
    currentValue = destination.remove(0);
    return true;
  }
}