/**
 * Copyright 2013 Criteo.
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
package parquet.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.ParquetInputSplit;
import parquet.hive.read.MapWritableReadSupport;

/**
*
* A Parquet InputFormat for Hive (with the deprecated package mapred)
*
*
* @author Mickaël Lacour <m.lacour@criteo.com>
* @author Rémy Pecqueur <r.pecqueur@criteo.com>
*
*/
@SuppressWarnings({ "unchecked", "rawtypes" })
public class DeprecatedParquetInputFormat<V> extends FileInputFormat<Void, V> {

    protected ParquetInputFormat<MapWritableReadSupport> realInput;

    public DeprecatedParquetInputFormat() {
        this.realInput = new ParquetInputFormat(MapWritableReadSupport.class);
    }

    public DeprecatedParquetInputFormat(InputFormat<Void, V> realInputFormat) {
        this.realInput = (ParquetInputFormat<MapWritableReadSupport>) realInputFormat;
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public org.apache.hadoop.mapred.InputSplit[] getSplits(org.apache.hadoop.mapred.JobConf job, int numSplits) throws IOException {

        List<org.apache.hadoop.mapreduce.InputSplit> splits = realInput.getSplits(new JobContext(job, null));

        if (splits == null) {
            return null;
        }

        InputSplit[] resultSplits = new InputSplit[splits.size()];
        int i = 0;

        for (org.apache.hadoop.mapreduce.InputSplit split : splits) {
            try {
                resultSplits[i++] = new InputSplitWrapper((ParquetInputSplit) split);
            } catch (InterruptedException e) {
                return null;
            }
        }

        return resultSplits;
    }

    @Override
    public org.apache.hadoop.mapred.RecordReader<Void, V> getRecordReader(org.apache.hadoop.mapred.InputSplit split, org.apache.hadoop.mapred.JobConf job,
            org.apache.hadoop.mapred.Reporter reporter) throws IOException {
        initInputFormat(job);
        return (RecordReader<Void, V>) new RecordReaderWrapper<MapWritableReadSupport>(realInput, split, job, reporter);
    }

    private void initInputFormat(JobConf conf) {
        if (realInput == null) {
            realInput = new ParquetInputFormat(MapWritableReadSupport.class);
        }
    }

    private static class InputSplitWrapper extends FileSplit implements InputSplit {

        private ParquetInputSplit realSplit;

        public org.apache.hadoop.mapreduce.InputSplit getRealSplit() {
            return realSplit;
        }

        // MapReduce instantiates this.
        public InputSplitWrapper() {
            super((Path) null, 0, 0, (String[]) null);
        }

        public InputSplitWrapper(ParquetInputSplit realSplit) throws IOException, InterruptedException {
            super(realSplit.getPath(), realSplit.getStart(), realSplit.getLength(), realSplit.getLocations());
            this.realSplit = realSplit;
        }

        @Override
        public long getLength() {
            try {
                return realSplit.getLength();
            } catch (InterruptedException e) {
                return 0;
            } catch (IOException e) {
                return 0;
            }
        }

        @Override
        public String[] getLocations() throws IOException {
            try {
                return realSplit.getLocations();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            String className = WritableUtils.readString(in);
            Class<?> splitClass;

            try {
                splitClass = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }

            realSplit = (ParquetInputSplit) ReflectionUtils.newInstance(splitClass, null);
            ((Writable) realSplit).readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException {
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

    private static class RecordReaderWrapper<V> implements RecordReader<Void, V> {

        private org.apache.hadoop.mapreduce.RecordReader<Void, V> realReader;
        private long splitLen; // for getPos()

        // expect readReader return same Key & Value objects (common case)
        // this avoids extra serialization & deserialization of these objects
        private V valueObj = null;

        private boolean firstRecord = false;
        private boolean eof = false;

        public RecordReaderWrapper(ParquetInputFormat<V> newInputFormat, InputSplit oldSplit, JobConf oldJobConf, Reporter reporter) throws IOException {

            splitLen = oldSplit.getLength();

            ParquetInputSplit split;

            if (oldSplit instanceof InputSplitWrapper)
                split = (ParquetInputSplit) ((InputSplitWrapper) oldSplit).getRealSplit();
            else if (oldSplit instanceof FileSplit) {
                throw new NotImplementedException("Use 'set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat' for now");
            } else
                throw new RuntimeException("Unknown split type");

            TaskAttemptID taskAttemptID = TaskAttemptID.forName(oldJobConf.get("mapred.task.id"));
            if (taskAttemptID == null) {
                taskAttemptID = new TaskAttemptID();
            }

            // create a TaskInputOutputContext
            TaskAttemptContext taskContext = new TaskInputOutputContext(oldJobConf, taskAttemptID, null, null, new ReporterWrapper(reporter)) {

                public Object getCurrentKey() throws IOException, InterruptedException {
                    throw new NotImplementedException();
                }

                public Object getCurrentValue() throws IOException, InterruptedException {
                    throw new NotImplementedException();
                }

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
            } catch (InterruptedException e) {
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
        public V createValue() {
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
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public boolean next(Void key, V value) throws IOException {
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

                        throw new IOException("DeprecatedParquetHiveInput can not " + "support RecordReaders that don't return same key & value "
                                + "objects. current reader class : " + realReader.getClass());
                    }

                    return true;
                }
            } catch (InterruptedException e) {
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
        private Reporter wrappedReporter;

        public ReporterWrapper(Reporter reporter) {
            wrappedReporter = reporter;
        }

        @Override
        public Counters.Counter getCounter(Enum<?> anEnum) {
            return wrappedReporter.getCounter(anEnum);
        }

        @Override
        public Counters.Counter getCounter(String s, String s1) {
            return wrappedReporter.getCounter(s, s1);
        }

        @Override
        public void incrCounter(Enum<?> anEnum, long l) {
            wrappedReporter.incrCounter(anEnum, l);
        }

        @Override
        public void incrCounter(String s, String s1, long l) {
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
        public void setStatus(String s) {
            wrappedReporter.setStatus(s);
        }
    }

}
