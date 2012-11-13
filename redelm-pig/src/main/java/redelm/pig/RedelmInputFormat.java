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

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import redelm.column.mem.MemColumnsStore;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.io.RecordConsumer;
import redelm.parser.MessageTypeParser;
import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.Type;

public class RedelmInputFormat<T> extends FileInputFormat<Void, T> {
  private static final Logger LOG = Logger.getLogger(RedelmInputFormat.class);

  private ReadSupport<T> readSupport;
  private MessageType requestedSchema;


  public <S extends ReadSupport<T>> RedelmInputFormat(Class<S> recordSupportClass, String requestedSchema) {
    try {
      this.readSupport = recordSupportClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("could not instantiate " + recordSupportClass.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal access to class " + recordSupportClass.getName(), e);
    }
    this.requestedSchema = requestedSchema == null ? null : MessageTypeParser.parseMessageType(requestedSchema);

  }

  @Override
  public RecordReader<Void, T> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    return new RecordReader<Void, T>() {
      private ColumnIOFactory columnIOFactory = new ColumnIOFactory();
      private Map<String, ColumnMetaData> columnTypes = new HashMap<String, ColumnMetaData>();

      private MessageType schema;

      private T currentValue;
      private int total;
      private int current;

      private RedelmFileReader reader;
      private List<T> destination = new LinkedList<T>();
      private redelm.io.RecordReader recordReader;
      private MemColumnsStore columnsStore;
      private RecordConsumer recordConsumer;
      private FSDataInputStream f;

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
          recordConsumer = readSupport.newRecordConsumer(destination);
        }
      }

      @Override
      public void close() throws IOException {
        f.close();
      }

      @Override
      public Void getCurrentKey() throws IOException, InterruptedException {
        return null;
      }

      @Override
      public T getCurrentValue() throws IOException,
      InterruptedException {
        return currentValue;
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
        schema = MessageTypeParser.parseMessageType(redelmInputSplit.getSchema());
        if (requestedSchema == null) {
          requestedSchema = schema;
        }
        readSupport.initForRead(redelmInputSplit, requestedSchema);
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
          currentValue = null;
          return false;
        }
        current ++;
        currentValue = destination.remove(0);
        return true;
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> statuses = super.listStatus(jobContext);
    LOG.debug("reading " + statuses.size() + " files");
    FileSystem fs = FileSystem.get(jobContext.getConfiguration());
    for (FileStatus fileStatus : statuses) {
      LOG.debug(fileStatus.getPath());
      FSDataInputStream f = fs.open(fileStatus.getPath());
      List<MetaDataBlock> metaDataBlocks = RedelmFileReader.readFooter(f, fileStatus.getLen());
      Footer footer = Footer.fromMetaDataBlocks(metaDataBlocks);
      List<BlockMetaData> blocks = footer.getBlocks();
      for (BlockMetaData block : blocks) {
        long startIndex = block.getStartIndex();
        long length = block.getEndIndex() - startIndex;
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, startIndex, length);
        List<String> hosts = new ArrayList<String>();
        for (BlockLocation blockLocation : fileBlockLocations) {
          hosts.addAll(Arrays.asList(blockLocation.getHosts()));
        }
        PigMetaData pigMetaData = PigMetaData.fromMetaDataBlocks(metaDataBlocks);
        splits.add(
            new RedelmInputSplit(
                fileStatus.getPath(),
                startIndex,
                length,
                hosts.toArray(new String[hosts.size()]),
                block,
                footer.getSchema(),
                pigMetaData == null ? null : pigMetaData.getPigSchema(),
                footer.getCodecClassName()));
      }
    }
    return splits;
  }
}
