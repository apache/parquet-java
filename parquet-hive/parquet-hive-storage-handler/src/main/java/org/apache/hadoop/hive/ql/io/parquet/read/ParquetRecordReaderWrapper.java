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
package org.apache.hadoop.hive.ql.io.parquet.read;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hive.HiveBinding;
import org.apache.parquet.hive.HiveBindingFactory;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetRecordReaderWrapper implements RecordReader<Void, ArrayWritable> {
  public static final Logger LOG = LoggerFactory.getLogger(ParquetRecordReaderWrapper.class);

  private final long splitLen; // for getPos()

  private org.apache.hadoop.mapreduce.RecordReader<Void, ArrayWritable> realReader;
  // expect readReader return same Key & Value objects (common case)
  // this avoids extra serialization & deserialization of these objects
  private ArrayWritable valueObj = null;
  private boolean firstRecord = false;
  private boolean eof = false;
  private int schemaSize;

  private final HiveBinding hiveBinding;

  public ParquetRecordReaderWrapper(final ParquetInputFormat<ArrayWritable> newInputFormat, final InputSplit oldSplit,
      final JobConf oldJobConf, final Reporter reporter) throws IOException, InterruptedException {
    this(newInputFormat, oldSplit, oldJobConf, reporter, (new HiveBindingFactory()).create());
  }

  public ParquetRecordReaderWrapper(final ParquetInputFormat<ArrayWritable> newInputFormat, final InputSplit oldSplit,
      final JobConf oldJobConf, final Reporter reporter, final HiveBinding hiveBinding)
      throws IOException, InterruptedException {
    this.splitLen = oldSplit.getLength();
    this.hiveBinding = hiveBinding;

    final ParquetInputSplit split = getSplit(oldSplit, oldJobConf);

    TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get(IOConstants.MAPRED_TASK_ID));
    if (taskAttemptID == null) {
      taskAttemptID = new TaskAttemptID();
    }

    // create a TaskInputOutputContext
    final TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(oldJobConf, taskAttemptID);

    if (split != null) {
      try {
        realReader = newInputFormat.createRecordReader(split, taskContext);
        realReader.initialize(split, taskContext);

        // read once to gain access to key and value objects
        if (realReader.nextKeyValue()) {
          firstRecord = true;
          valueObj = realReader.getCurrentValue();
        } else {
          eof = true;
        }
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    } else {
      realReader = null;
      eof = true;
    }
    if (valueObj == null) { // Should initialize the value for createValue
      valueObj = new ArrayWritable(Writable.class, new Writable[schemaSize]);
    }
  }

  @Override
  public void close() throws IOException {
    if (realReader != null) {
      realReader.close();
    }
  }

  @Override
  public Void createKey() {
    return null;
  }

  @Override
  public ArrayWritable createValue() {
    return valueObj;
  }

  @Override
  public long getPos() throws IOException {
    return (long) (splitLen * getProgress());
  }

  @Override
  public float getProgress() throws IOException {
    if (realReader == null) {
      return 1f;
    } else {
      try {
        return realReader.getProgress();
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean next(final Void key, final ArrayWritable value) throws IOException {
    if (eof) {
      return false;
    }
    try {
      if (firstRecord) { // key & value are already read.
        firstRecord = false;
      } else if (!realReader.nextKeyValue()) {
        eof = true; // strictly not required, just for consistency
        return false;
      }

      final ArrayWritable tmpCurValue = realReader.getCurrentValue();
      if (value != tmpCurValue) {
        final Writable[] arrValue = value.get();
        final Writable[] arrCurrent = tmpCurValue.get();
        if (value != null && arrValue.length == arrCurrent.length) {
          System.arraycopy(arrCurrent, 0, arrValue, 0, arrCurrent.length);
        } else {
          if (arrValue.length != arrCurrent.length) {
            throw new IOException("DeprecatedParquetHiveInput : size of object differs. Value" + " size :  "
                + arrValue.length + ", Current Object size : " + arrCurrent.length);
          } else {
            throw new IOException("DeprecatedParquetHiveInput can not support RecordReaders that"
                + " don't return same key & value & value is null");
          }
        }
      }
      return true;
    } catch (final InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * gets a ParquetInputSplit corresponding to a split given by Hive
   *
   * @param oldSplit The split given by Hive
   * @param conf The JobConf of the Hive job
   * @return a ParquetInputSplit corresponding to the oldSplit
   * @throws IOException if the config cannot be enhanced or if the footer cannot
   * be read from the file
   */
  protected ParquetInputSplit getSplit(final InputSplit oldSplit, final JobConf conf) throws IOException {
    if (oldSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) oldSplit;
      final long splitStart = fileSplit.getStart();
      final long splitLength = fileSplit.getLength();
      final Path finalPath = fileSplit.getPath();
      final JobConf cloneJob = hiveBinding.pushProjectionsAndFilters(conf, finalPath.getParent());

      final ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(cloneJob, finalPath, SKIP_ROW_GROUPS);
      final FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
      final ReadContext readContext = new DataWritableReadSupport().init(cloneJob, fileMetaData.getKeyValueMetaData(),
          fileMetaData.getSchema());

      schemaSize = MessageTypeParser
          .parseMessageType(readContext.getReadSupportMetadata().get(DataWritableReadSupport.HIVE_SCHEMA_KEY))
          .getFieldCount();
      return new ParquetInputSplit(finalPath, splitStart, splitStart + splitLength, splitLength,
          fileSplit.getLocations(), null);
    } else {
      throw new IllegalArgumentException("Unknown split type: " + oldSplit);
    }
  }
}
