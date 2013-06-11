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
import org.apache.hadoop.io.ArrayWritable;
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
import parquet.hive.write.DataWritableWriteSupport;

/**
 *
 * A Parquet OutputFormat for Hive (with the deprecated package mapred)
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 * @author Rémy Pecqueur <r.pecqueur@criteo.com>
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class DeprecatedParquetOutputFormat extends FileOutputFormat<Void, ArrayWritable> implements HiveOutputFormat<Void, ArrayWritable> {

  protected ParquetOutputFormat<ArrayWritable> realOutputFormat;

  public DeprecatedParquetOutputFormat() {
    realOutputFormat = new ParquetOutputFormat<ArrayWritable>(new DataWritableWriteSupport());
  }

  public DeprecatedParquetOutputFormat(final OutputFormat<Void, ArrayWritable> mapreduceOutputFormat) {
    realOutputFormat = (ParquetOutputFormat<ArrayWritable>) mapreduceOutputFormat;
  }

  @Override
  public void checkOutputSpecs(final FileSystem ignored, final JobConf job) throws IOException {
    realOutputFormat.checkOutputSpecs(new JobContext(job, null));
  }

  @Override
  public RecordWriter<Void, ArrayWritable> getRecordWriter(final FileSystem ignored, final JobConf job, final String name, final Progressable progress) throws IOException {
    throw new RuntimeException("Should never be used");
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(final JobConf jc, final Path finalOutPath, final Class<? extends Writable> valueClass,
          final boolean isCompressed, final Properties tableProperties, final Progressable progress) throws IOException {
    // TODO find out if we can overwrite any _col0 column names here
    final String columnNameProperty = tableProperties.getProperty("columns");
    final String columnTypeProperty = tableProperties.getProperty("columns.types");
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

    DataWritableWriteSupport.setSchema(HiveSchemaConverter.convert(columnNames, columnTypes), jc);
    return new RecordWriterWrapper(realOutputFormat, jc, finalOutPath.toString(), progress);
  }

  private static class RecordWriterWrapper implements RecordWriter<Void, ArrayWritable>, org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter {

    private org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable> realWriter;
    private TaskAttemptContext taskContext;

    RecordWriterWrapper(final OutputFormat<Void, ArrayWritable> realOutputFormat, final JobConf jobConf, final String name, final Progressable progress) throws IOException {
      try {
        // create a TaskInputOutputContext
        taskContext = new TaskInputOutputContext(jobConf, TaskAttemptID.forName(jobConf.get("mapred.task.id")), null, null, (StatusReporter) progress) {
          @Override
          public Object getCurrentKey() throws IOException, InterruptedException {
            throw new RuntimeException("not implemented");
          }

          @Override
          public Object getCurrentValue() throws IOException, InterruptedException {
            throw new RuntimeException("not implemented");
          }

          @Override
          public boolean nextKeyValue() throws IOException, InterruptedException {
            throw new RuntimeException("not implemented");
          }
        };

        realWriter = (org.apache.hadoop.mapreduce.RecordWriter<Void, ArrayWritable>) ((ParquetOutputFormat) realOutputFormat).getRecordWriter(taskContext, new Path(name));
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(final Reporter reporter) throws IOException {
      try {
        // create a context just to pass reporter
        realWriter.close(taskContext);
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void write(final Void key, final ArrayWritable value) throws IOException {
      try {
        realWriter.write(key, value);
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(final boolean abort) throws IOException {
      try {
        realWriter.close(taskContext);
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void write(final Writable w) throws IOException {
      try {
        realWriter.write(null, (ArrayWritable) w);
      } catch (final InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
}
