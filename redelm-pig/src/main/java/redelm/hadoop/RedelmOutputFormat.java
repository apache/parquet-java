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
import java.util.Collection;
import java.util.List;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnWriter;
import redelm.column.mem.MemColumn;
import redelm.column.mem.MemColumnsStore;
import redelm.io.ColumnIOFactory;
import redelm.io.MessageColumnIO;
import redelm.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RedelmOutputFormat<T> extends FileOutputFormat<Void, T> {

  // TODO: make this configurable
  private static final int THRESHOLD = 1024*1024*50;

  private MemColumnsStore store;

  private final MessageType schema;
  private final String codecClassName;
  private WriteSupport<T> writeSupport;

  private final List<MetaDataBlock> extraMetaData;

  public <S extends WriteSupport<T>> RedelmOutputFormat(Class<S> writeSupportClass, MessageType schema, List<MetaDataBlock> extraMetaData, String codecClassName) {
    try {
      this.writeSupport = writeSupportClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("could not instantiate " + writeSupportClass.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Illegal access to class " + writeSupportClass.getName(), e);
    }
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.codecClassName = codecClassName;
    initStore();
  }

  private void initStore() {
    store = new MemColumnsStore(1024 * 1024 * 16);
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(this.schema, store);
    writeSupport.initForWrite(columnIO.getRecordWriter(), this.schema);
  }

  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    final Path file = getDefaultWorkFile(taskAttemptContext, "");
    final Configuration conf = taskAttemptContext.getConfiguration();
    final FileSystem fs = file.getFileSystem(conf);
    final RedelmFileWriter w = new RedelmFileWriter(schema, fs.create(file, false), codecClassName);
    w.start();
    return new RecordWriter<Void, T>() {
      private int recordCount;

      @Override
      public void close(TaskAttemptContext taskAttemptContext) throws IOException,
      InterruptedException {
        flushStore();
        w.end(extraMetaData);
      }

      @Override
      public void write(Void key, T value) throws IOException, InterruptedException {
        writeSupport.write(value);
        ++ recordCount;
        checkBlockSizeReached();
      }

      private void checkBlockSizeReached() throws IOException {
        if (store.memSize() > THRESHOLD) {
          flushStore();
          initStore();
        }
      }

      private void flushStore()
          throws IOException {
        w.startBlock(recordCount);
        Collection<MemColumn> columns = store.getColumns();
        for (MemColumn column : columns) {
          ColumnDescriptor descriptor = column.getDescriptor();
          ColumnWriter columnWriter = column.getColumnWriter();
          w.startColumn(descriptor, columnWriter.getValueCount());
          w.startRepetitionLevels();
          columnWriter.writeRepetitionLevelColumn(w);
          w.startDefinitionLevels();
          columnWriter.writeDefinitionLevelColumn(w);
          w.startData();
          columnWriter.writeDataColumn(w);
          w.endColumn();
        }
        recordCount = 0;
        w.endBlock();
        store = null;
        writeSupport = null;
      }
    };
  }

}
