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
package redelm.pig;

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
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFileInputFormat;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;

public class RedelmInputFormat extends PigFileInputFormat<Object, Tuple> {
  private static final Logger LOG = Logger.getLogger(RedelmInputFormat.class);

  private MessageType requestedSchema;

  public RedelmInputFormat(String requestedSchema) {
    this.requestedSchema = requestedSchema == null ? null : MessageType.parse(requestedSchema);
  }

  @Override
  public RecordReader<Object, Tuple> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new RecordReader<Object, Tuple>() {
      private ColumnIOFactory columnIOFactory = new ColumnIOFactory();
      private Map<String, ColumnMetaData> columnTypes = new HashMap<String, ColumnMetaData>();

      private MessageType schema;

      private Tuple currentTuple;
      private int total;
      private int current;

      private RedelmFileReader reader;
      private List<Tuple> destination = new LinkedList<Tuple>();
      private redelm.io.RecordReader recordReader;
      private MemColumnsStore columnsStore;
      private TupleRecordConsumer recordConsumer;
      private FSDataInputStream f;
      private Schema pigSchema;

      private void checkRead() throws IOException {
        if(columnsStore == null || columnsStore.isFullyConsumed()) {
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
          recordConsumer = new TupleRecordConsumer(requestedSchema, pigSchema, destination);
        }
      }

      @Override
      public void close() throws IOException {
        f.close();
      }

      @Override
      public Object getCurrentKey() throws IOException,
      InterruptedException {
        return null;
      }

      @Override
      public Tuple getCurrentValue() throws IOException,
      InterruptedException {
        return currentTuple;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return (float)current/total;
      }

      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(taskAttemptContext.getConfiguration());
        RedelmInputSplit redelmInputSplit = (RedelmInputSplit)inputSplit;
        Path path = redelmInputSplit.getPath();
        schema = MessageType.parse(redelmInputSplit.getSchema());
        LOG.debug(redelmInputSplit.getPigSchema());
        pigSchema = Utils.getSchemaFromString(redelmInputSplit.getPigSchema());
        if (requestedSchema == null) {
          requestedSchema = schema;
        }
        f = fs.open(path);
        BlockMetaData block = redelmInputSplit.getBlock();
        List<String[]> columns = new ArrayList<String[]>();
        for (ColumnMetaData columnMetaData : block.getColumns()) {
          columnTypes.put(Arrays.toString(columnMetaData.getPath()), columnMetaData);
          if (contains(requestedSchema, columnMetaData.getPath())) {
            columns.add(columnMetaData.getPath());
          }
        }
        reader = new RedelmFileReader(f, Arrays.asList(block), columns, redelmInputSplit.getCodecClassName());
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

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        checkRead();
        if (destination.size() == 0 && current<total) {
          recordReader.read(recordConsumer);
        }
        if (destination.size() == 0) {
          currentTuple = null;
          return false;
        }
        current ++;
        currentTuple = destination.remove(0);
        return true;
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> statuses = super.listStatus(jobContext);
    LOG.info(statuses);
    FileSystem fs = FileSystem.get(jobContext.getConfiguration());
    for (FileStatus fileStatus : statuses) {
      LOG.info(fileStatus);
      FSDataInputStream f = fs.open(fileStatus.getPath());
      Footer footer = RedelmFileReader.readFooter(f, fileStatus.getLen());
      List<BlockMetaData> blocks = footer.getBlocks();
      for (BlockMetaData block : blocks) {
//        List<ColumnMetaData> columns = block.getColumns();
//        // TODO: allow filtering of columns
//        List<String[]> paths = new ArrayList<String[]>();
        long startIndex = block.getStartIndex();
        long length = block.getEndIndex() - startIndex;
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, startIndex, length);
        List<String> hosts = new ArrayList<String>();
        for (BlockLocation blockLocation : fileBlockLocations) {
          hosts.addAll(Arrays.asList(blockLocation.getHosts()));
        }

        splits.add(
            new RedelmInputSplit(
                fileStatus.getPath(),
                startIndex,
                length,
                hosts.toArray(new String[hosts.size()]),
                block,
                footer.getSchema(),
                footer.getPigSchema(),
                footer.getCodecClassName()));
      }
    }
    return splits;
  }
}