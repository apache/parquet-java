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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.MapWritable;
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

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hive.read.MapWritableReadSupport;
import parquet.schema.MessageType;
import parquet.schema.Type;

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
public class DeprecatedParquetInputFormat extends FileInputFormat<Void, MapWritable> {

  protected ParquetInputFormat<MapWritable> realInput;

  public DeprecatedParquetInputFormat() {
    this.realInput = new ParquetInputFormat<MapWritable>(MapWritableReadSupport.class);
  }

  public DeprecatedParquetInputFormat(final InputFormat<Void, MapWritable> realInputFormat) {
    this.realInput = (ParquetInputFormat<MapWritable>) realInputFormat;
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
  public org.apache.hadoop.mapred.RecordReader<Void, MapWritable> getRecordReader(final org.apache.hadoop.mapred.InputSplit split, final org.apache.hadoop.mapred.JobConf job,
          final org.apache.hadoop.mapred.Reporter reporter) throws IOException {
    return (RecordReader<Void, MapWritable>) new RecordReaderWrapper(realInput, split, job, reporter);
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

  private static class RecordReaderWrapper implements RecordReader<Void, MapWritable> {

    private org.apache.hadoop.mapreduce.RecordReader<Void, MapWritable> realReader;
    private final long splitLen; // for getPos()
    // expect readReader return same Key & Value objects (common case)
    // this avoids extra serialization & deserialization of these objects
    private MapWritable valueObj = null;
    private final ManageJobConfig manageJob = new ManageJobConfig();
    private boolean firstRecord = false;
    private boolean eof = false;

    public RecordReaderWrapper(final ParquetInputFormat<MapWritable> newInputFormat, final InputSplit oldSplit, final JobConf oldJobConf, final Reporter reporter) throws IOException {

      splitLen = oldSplit.getLength();
      ParquetInputSplit split;

      if (oldSplit instanceof InputSplitWrapper) {
        split = ((InputSplitWrapper) oldSplit).getRealSplit();
      } else if (oldSplit instanceof FileSplit) {
        final Path finalPath = ((FileSplit) oldSplit).getPath();
        final JobConf cloneJob = manageJob.cloneJobAndInit(oldJobConf, finalPath.getParent());
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

        split = new ParquetInputSplit(finalPath, ((FileSplit) oldSplit).getStart(), oldSplit.getLength(), oldSplit.getLocations(), blocks,
                fileSchema.toString(), requestedSchemaByUser.toString(), fileMetaData.getKeyValueMetaData());

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
    }

    @Override
    public void close() throws IOException {
      realReader.close();
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public MapWritable createValue() {
      return valueObj;
    }

    @Override
    public long getPos() throws IOException {
      return (long) (splitLen * getProgress());
    }

    @Override
    public float getProgress() throws IOException {
      try {
        return realReader.getProgress();
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public boolean next(final Void key, final MapWritable value) throws IOException {
      if (eof) {
        return false;
      }

      if (firstRecord) { // key & value are already read.
        firstRecord = false;
        return true;
      }

      try {
        if (realReader.nextKeyValue()) {
          if (key != realReader.getCurrentKey() || value != realReader.getCurrentValue()) {
            throw new IOException("DeprecatedParquetHiveInput can not " + "support RecordReaders that don't return same key & value ");
          }

          return true;
        }
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }

      eof = true; // strictly not required, just for consistency
      return false;
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
  }
}
