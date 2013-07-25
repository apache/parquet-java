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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Progressable;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hive.read.DataWritableReadSupport;
import parquet.schema.MessageType;

/**
 *
 * TestHiveOuputFormat
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class TestDeprecatedParquetOuputFormat extends TestCase {

  Map<Integer, ArrayWritable> mapData;
  Configuration conf;
  JobConf job;
  FileSystem fs;
  Path dir;
  File testFile;
  Reporter reporter;
  FSDataOutputStream ds;

  @Override
  protected void setUp() throws Exception {
    conf = new Configuration();
    job = new JobConf(conf);
    fs = FileSystem.getLocal(conf);
    dir = new Path("target/tests/from_java/deprecatedoutputformat/");
    testFile = new File(dir.toString(), "customer");
    if (testFile.exists()) {
      if (!testFile.delete()) {
        throw new RuntimeException("can not remove existing file " + testFile.getAbsolutePath());
      }
    }
    reporter = Reporter.NULL;
    mapData = new HashMap<Integer, ArrayWritable>();
    mapData.clear();
    for (int i = 0; i < 1000; i++) {
      // yeah same test as pig one :)
      mapData.put(i, UtilitiesTestMethods.createArrayWritable(i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
              i % 13 == 0 ? null : i, i % 14 == 0 ? null : "phone_" + i, i % 15 == 0 ? null : 1.2d * i, i % 16 == 0 ? null : "mktsegment_" + i,
              i % 17 == 0 ? null : "comment_" + i));
    }
  }

  public void testParquetHiveOutputFormat() throws Exception {
    final HiveOutputFormat format = new DeprecatedParquetOutputFormat();
    final Properties tableProperties = new Properties();

    // Set the configuration parameters
    tableProperties.setProperty("columns",
            "c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment");
    tableProperties.setProperty("columns.types",
            "int:string:string:int:string:double:string:string");
    tableProperties.setProperty(org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT, "NULL");

    System.out.println("First part, write the data");

    job.set("mapred.task.id", "attempt_201304241759_32973_m_000002_0"); // FAKE ID
    fakeStatus reporter = new fakeStatus();
    org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter recordWriter = format.getHiveRecordWriter(
            job,
            new Path(testFile.getAbsolutePath()),
            NullWritable.class,
            false,
            tableProperties,
            reporter);
    // create key/value
    for (Map.Entry<Integer, ArrayWritable> entry : mapData.entrySet()) {
      recordWriter.write(entry.getValue());
    }
    recordWriter.close(false);
    assertTrue("File not created", testFile.exists());

    System.out.println("Second part, check if everything is ok");

    checkWrite();
  }

  private void checkWrite() throws IOException, InterruptedException {
    final ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(testFile.getAbsolutePath()));
    final MessageType schema = readFooter.getFileMetaData().getSchema();

    long size = 0;
    final List<BlockMetaData> blocks = readFooter.getBlocks();
    for (final BlockMetaData block : blocks) {
      size += block.getTotalByteSize();
    }


    final FileInputFormat<Void, ArrayWritable> format = new DeprecatedParquetInputFormat();
    final String[] locations = new String[] {"localhost"};
    final String schemaToString = schema.toString();
    final String columnsStr = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_name;\n"
            + "  optional binary c_address;\n"
            + "  optional int32 c_nationkey;\n"
            + "  optional binary c_phone;\n"
            + "  optional double c_acctbal;\n"
            + "  optional binary c_mktsegment;\n"
            + "  optional binary c_comment;\n"
            + "}";


    final Map<String, String> readSupportMetaData = new HashMap<String, String>();
    readSupportMetaData.put(DataWritableReadSupport.HIVE_SCHEMA_KEY, columnsStr);
    final ParquetInputSplit realSplit = new ParquetInputSplit(new Path(testFile.getAbsolutePath()), 0, size, locations, blocks,
            schemaToString, schemaToString, readFooter.getFileMetaData().getKeyValueMetaData(), readSupportMetaData);

    final DeprecatedParquetInputFormat.InputSplitWrapper splitWrapper = new DeprecatedParquetInputFormat.InputSplitWrapper(realSplit);

    // construct the record reader
    final RecordReader<Void, ArrayWritable> reader = format.getRecordReader(splitWrapper, job, reporter);

    // create key/value
    final Void key = reader.createKey();
    final ArrayWritable value = reader.createValue();


    int count = 0;
    while (reader.next(key, value)) {
      assertTrue(count < mapData.size());
      assertTrue(key == null);
      final Writable[] arrValue = value.get();
      final Writable[] writableArr = arrValue;
      final ArrayWritable expected = mapData.get(((IntWritable) writableArr[0]).get());
      final Writable[] arrExpected = expected.get();
      assertEquals(arrValue.length, 8);

      final boolean deepEquals = UtilitiesTestMethods.smartCheckArray(arrValue, arrExpected, new Integer[] {0, 1, 2, 3, 4, 5, 6, 7});

      assertTrue(deepEquals);
      count++;
    }
    reader.close();

    assertEquals("Number of lines found and data written don't match", count, mapData.size());

  }

  // FAKE Class in order to compile
  private class fakeStatus extends org.apache.hadoop.mapreduce.StatusReporter implements Progressable {

    @Override
    public Counter getCounter(Enum<?> e) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Counter getCounter(String string, String string1) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void progress() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setStatus(String string) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public float getProgress() {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  }
}
