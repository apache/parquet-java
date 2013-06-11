/**
 * Copyright 2013 Criteo.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License
 * at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package parquet.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;

import org.apache.hadoop.io.ArrayWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import parquet.Log;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.Type;
import parquet.hive.read.DataWritableReadSupport;
import parquet.schema.MessageTypeParser;

/**
 *
 * A Parquet InputFormat for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class DeprecatedParquetInputFormat extends FileInputFormat<Void, ArrayWritable> {

  private static final Log LOG = Log.getLog(DeprecatedParquetInputFormat.class);
  protected ParquetInputFormat<ArrayWritable> realInput;

  public DeprecatedParquetInputFormat() {
    this.realInput = new ParquetInputFormat<ArrayWritable>(DataWritableReadSupport.class);
  }

  public DeprecatedParquetInputFormat(final InputFormat<Void, ArrayWritable> realInputFormat) {
    this.realInput = (ParquetInputFormat<ArrayWritable>) realInputFormat;
  }

  @Override
  protected boolean isSplitable(final FileSystem fs, final Path filename) {
    return false;
  }
  private final ManageJobConfig manageJob = new ManageJobConfig();

  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(final org.apache.hadoop.mapred.JobConf job, final int numSplits) throws IOException {
    final Path[] dirs = FileInputFormat.getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    final Path tmpPath = new Path((dirs[dirs.length - 1]).makeQualified(FileSystem.get(job)).toUri().getPath());
    final JobConf cloneJobConf = manageJob.cloneJobAndInit(job, tmpPath);
    final List<org.apache.hadoop.mapreduce.InputSplit> splits = realInput.getSplits(new JobContext(cloneJobConf, null));

    if (splits == null) {
      return null;
    }

    final InputSplit[] resultSplits = new InputSplit[splits.size()];
    int i = 0;

    for (final org.apache.hadoop.mapreduce.InputSplit split : splits) {
      try {
        resultSplits[i++] = new InputSplitWrapper((ParquetInputSplit) split);
      } catch (final InterruptedException e) {
        return null;
      }
    }

    return resultSplits;
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader<Void, ArrayWritable> getRecordReader(final org.apache.hadoop.mapred.InputSplit split,
          final org.apache.hadoop.mapred.JobConf job, final org.apache.hadoop.mapred.Reporter reporter) throws IOException {
    try {
      return (RecordReader<Void, ArrayWritable>) new RecordReaderWrapper(realInput, split, job, reporter);
    } catch (final InterruptedException e) {
      e.printStackTrace();
      return null;
    }
  }

  static class InputSplitWrapper extends FileSplit implements InputSplit {

    private ParquetInputSplit realSplit;

    public ParquetInputSplit getRealSplit() {
      return realSplit;
    }

    // MapReduce instantiates this.
    public InputSplitWrapper() {
      super((Path) null, 0, 0, (String[]) null);
    }

    public InputSplitWrapper(final ParquetInputSplit realSplit) throws IOException, InterruptedException {
      super(realSplit.getPath(), realSplit.getStart(), realSplit.getLength(), realSplit.getLocations());
      this.realSplit = realSplit;
    }

    @Override
    public long getLength() {
      try {
        return realSplit.getLength();
      } catch (final InterruptedException e) {
        return 0;
      } catch (final IOException e) {
        return 0;
      }
    }

    @Override
    public String[] getLocations() throws IOException {
      try {
        return realSplit.getLocations();
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
      final String className = WritableUtils.readString(in);
      Class<?> splitClass;

      try {
        splitClass = Class.forName(className);
      } catch (final ClassNotFoundException e) {
        throw new IOException(e);
      }

      realSplit = (ParquetInputSplit) ReflectionUtils.newInstance(splitClass, null);
      ((Writable) realSplit).readFields(in);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
      WritableUtils.writeString(out, realSplit.getClass().getName());
      ((Writable) realSplit).write(out);
    }

    @Override
    public Path getPath() {
      return realSplit.getPath();
    }

    @Override
    public long getStart() {
      return realSplit.getStart();
    }
  }

  private static class RecordReaderWrapper implements RecordReader<Void, ArrayWritable> {

    private org.apache.hadoop.mapreduce.RecordReader<Void, ArrayWritable> realReader;
    private final long splitLen; // for getPos()
    // expect readReader return same Key & Value objects (common case)
    // this avoids extra serialization & deserialization of these objects
    private ArrayWritable valueObj = null;
    private final ManageJobConfig manageJob = new ManageJobConfig();
    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(final ParquetInputFormat<ArrayWritable> newInputFormat, final InputSplit oldSplit, final JobConf oldJobConf, final Reporter reporter)
            throws IOException, InterruptedException {

      splitLen = oldSplit.getLength();
      ParquetInputSplit split = null;
      final int sizeSchema;
      // TODO: put in InputSplitFactory(oldSplit)
      if (oldSplit instanceof InputSplitWrapper) {
        split = ((InputSplitWrapper) oldSplit).getRealSplit();
        sizeSchema = MessageTypeParser.parseMessageType(split.getRequestedSchema()).getColumns().size();
      } else if (oldSplit instanceof FileSplit) {
        final Path finalPath = ((FileSplit) oldSplit).getPath();
        final JobConf cloneJob = manageJob.cloneJobAndInit(oldJobConf, finalPath.getParent());

        final FileSystem fs = FileSystem.get(cloneJob);
        final FileStatus status = fs.getFileStatus(finalPath);

        final BlockLocation[] hdfsBlocks = fs.getFileBlockLocations(status, ((FileSplit) oldSplit).getStart(), oldSplit.getLength());
        if (hdfsBlocks.length != 1) {
          throw new RuntimeException("Should have exactly 1 HDFS block per split, got: " + hdfsBlocks.length);
        }
        final BlockLocation hdfsBlock = hdfsBlocks[0];

        final ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(cloneJob, finalPath);
        final List<BlockMetaData> blocks = parquetMetadata.getBlocks();
        final FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

        final List<String> listColumns = (List<String>) StringUtils.getStringCollection(cloneJob.get("columns"));
        final MessageType fileSchema = fileMetaData.getSchema();
        MessageType requestedSchemaByUser = fileSchema;
        final List<Integer> indexColumnsWanted = ColumnProjectionUtils.getReadColumnIDs(cloneJob);
        if (indexColumnsWanted.isEmpty() == false) {
          final List<Type> typeList = new ArrayList<Type>();
          for (final Integer idx : indexColumnsWanted) {
            typeList.add(fileSchema.getType(listColumns.get(idx)));
          }
          requestedSchemaByUser = new MessageType(fileSchema.getName(), typeList);
        }
        sizeSchema = requestedSchemaByUser.getColumns().size();
        final List<BlockMetaData> splitGroup = new ArrayList<BlockMetaData>();
        for (final BlockMetaData block : blocks) {
          final long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
          if (firstDataPage >= hdfsBlock.getOffset() && firstDataPage < hdfsBlock.getOffset() + hdfsBlock.getLength()) {
            splitGroup.add(block);
          }
        }

        if (splitGroup.size() == 0) {
          LOG.warn("Skipping split, could not find row group in: " + (FileSplit) oldSplit);
          split = null;
        } else {
          split = new ParquetInputSplit(finalPath, hdfsBlock.getOffset(), hdfsBlock.getLength(), hdfsBlock.getHosts(), splitGroup, fileMetaData.getSchema().toString(),
                  requestedSchemaByUser.toString(), fileMetaData.getKeyValueMetaData());
        }

      } else {
        throw new RuntimeException("Unknown split type");
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get("mapred.task.id"));
      if (taskAttemptID == null) {
        taskAttemptID = new TaskAttemptID();
      }

      // create a TaskInputOutputContext
      final TaskAttemptContext taskContext = new TaskInputOutputContext(oldJobConf, taskAttemptID, null, null, new ReporterWrapper(reporter)) {
        @Override
        public Object getCurrentKey() throws IOException, InterruptedException {
          throw new NotImplementedException();
        }

        @Override
        public Object getCurrentValue() throws IOException, InterruptedException {
          throw new NotImplementedException();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          throw new NotImplementedException();
        }
      };

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
        if (valueObj == null) {
          valueObj = new ArrayWritable(Writable.class, new Writable[sizeSchema]);
        }
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
          Writable[] arrValue = value.get();
          Writable[] arrCurrent = tmpCurValue.get();
          if (value != null && arrValue.length == arrCurrent.length) {
            System.arraycopy(arrCurrent, 0, arrValue, 0, arrCurrent.length);
          } else {
            if (arrValue.length != arrCurrent.length) {
              throw new IOException("DeprecatedParquetHiveInput : size of object differs. Value size :  " + arrValue.length + ", Current Object size : " + arrCurrent.length);
            } else {
              throw new IOException("DeprecatedParquetHiveInput can not support RecordReaders that don't return same key & value & value is null");
            }
          }
        }
        return true;

      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * A reporter that works with both mapred and mapreduce APIs.
   */
  private static class ReporterWrapper extends StatusReporter implements Reporter {

    private final Reporter wrappedReporter;

    public ReporterWrapper(final Reporter reporter) {
      wrappedReporter = reporter;
    }

    @Override
    public Counters.Counter getCounter(final Enum<?> anEnum) {
      return wrappedReporter.getCounter(anEnum);
    }

    @Override
    public Counters.Counter getCounter(final String s, final String s1) {
      return wrappedReporter.getCounter(s, s1);
    }

    @Override
    public void incrCounter(final Enum<?> anEnum, final long l) {
      wrappedReporter.incrCounter(anEnum, l);
    }

    @Override
    public void incrCounter(final String s, final String s1, final long l) {
      wrappedReporter.incrCounter(s, s1, l);
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return wrappedReporter.getInputSplit();
    }

    @Override
    public void progress() {
      wrappedReporter.progress();
    }

    @Override
    public void setStatus(final String s) {
      wrappedReporter.setStatus(s);
    }

    @Override
    public float getProgress() {
      return wrappedReporter.getProgress();
    }
  }
}
