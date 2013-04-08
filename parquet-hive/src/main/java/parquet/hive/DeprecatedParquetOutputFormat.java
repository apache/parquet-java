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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Progressable;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hive.convert.HiveSchemaConverter;
import parquet.hive.write.MapWritableWriteSupport;

/**
*
* A Parquet OutputFormat for Hive (with the deprecated package mapred)
*
*
* @author Mickaël Lacour <m.lacour@criteo.com>
* @author Rémy Pecqueur <r.pecqueur@criteo.com>
*
*/
@SuppressWarnings({ "unchecked", "rawtypes" })
public class DeprecatedParquetOutputFormat<V extends Writable> extends FileOutputFormat<Void, V> implements HiveOutputFormat<NullWritable, V> {

    protected ParquetOutputFormat<V> realOutputFormat;

    public DeprecatedParquetOutputFormat() {
    }

    public DeprecatedParquetOutputFormat(OutputFormat<Void, V> mapreduceOutputFormat) {
        realOutputFormat = (ParquetOutputFormat<V>) mapreduceOutputFormat;
    }

    private void initOutputFormat(JobConf conf) {
        if (realOutputFormat == null) {
            realOutputFormat = new ParquetOutputFormat<V>();
            conf.set(ParquetOutputFormat.WRITE_SUPPORT_CLASS, MapWritableWriteSupport.class.getName());
        }
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        initOutputFormat(job);
        realOutputFormat.checkOutputSpecs(new JobContext(job, null));
    }

    @Override
    public RecordWriter<Void, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        initOutputFormat(job);
        return new RecordWriterWrapper<Void, V>(realOutputFormat, job, name, progress);
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
            boolean isCompressed, Properties tableProperties, Progressable progress) throws IOException {
        initOutputFormat(jc);
        String columnNameProperty = tableProperties.getProperty("columns");
        String columnTypeProperty = tableProperties.getProperty("columns.types");
        List<String> columnNames;
        List<TypeInfo> columnTypes;

        if (columnNameProperty.length() == 0) {
            columnNames = new ArrayList<String>();
        } else {
            columnNames = Arrays.asList(columnNameProperty.split(","));
        }

        if (columnTypeProperty.length() == 0) {
            columnTypes = new ArrayList<TypeInfo>();
        } else {
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
        }

        MapWritableWriteSupport.setSchema(new HiveSchemaConverter().convert(columnNames, columnTypes), jc);
        return new RecordWriterWrapper<Void, V>(realOutputFormat, jc, finalOutPath.toString(), progress);
    }

    private static class RecordWriterWrapper<K, V> implements RecordWriter<K, V>, org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {

        private org.apache.hadoop.mapreduce.RecordWriter<K, V> realWriter;
        private TaskAttemptContext taskContext;

        RecordWriterWrapper(OutputFormat<Void, V> realOutputFormat, JobConf jobConf, String name, Progressable progress) throws IOException {
            try {
                // create a TaskInputOutputContext
                taskContext = new TaskInputOutputContext(jobConf, TaskAttemptID.forName(jobConf.get("mapred.task.id")), null, null, (StatusReporter) progress) {
                    public Object getCurrentKey() throws IOException, InterruptedException {
                        throw new RuntimeException("not implemented");
                    }

                    public Object getCurrentValue() throws IOException, InterruptedException {
                        throw new RuntimeException("not implemented");
                    }

                    public boolean nextKeyValue() throws IOException, InterruptedException {
                        throw new RuntimeException("not implemented");
                    }
                };

                realWriter = (org.apache.hadoop.mapreduce.RecordWriter<K, V>) ((ParquetOutputFormat) realOutputFormat).getRecordWriter(taskContext, new Path(name));
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            try {
                // create a context just to pass reporter
                realWriter.close(taskContext);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void write(K key, V value) throws IOException {
            try {
                realWriter.write(key, value);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void close(boolean abort) throws IOException {
            try {
                realWriter.close(taskContext);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void write(Writable w) throws IOException {
            try {
                realWriter.write(null, (V) w);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }
}
