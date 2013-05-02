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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Progressable;

/**
 *
 * TestHiveOuputFormat
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
public class TestDeprecatedParquetOuputFormat extends TestCase {

  Map<Integer, MapWritable> mapData;
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
    dir = new Path("testdata/from_java/deprecatedoutputformat/");
    testFile = new File(dir.toString(), "customer");
    if (testFile.exists()) {
      if (!testFile.delete()) {
        throw new RuntimeException("can not remove existing file " + testFile.getAbsolutePath());
      }
    }
    reporter = Reporter.NULL;
    mapData = new HashMap<Integer, MapWritable>();
    mapData.clear();
    for (int i = 0; i < 1000; i++) {
      // yeah same test as pig one :)
      mapData.put(i, UtilitiesTestMethods.createMap(i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
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
    for (Map.Entry<Integer, MapWritable> entry : mapData.entrySet()) {
      recordWriter.write(entry.getValue());
    }
    recordWriter.close(false);
    assertTrue("File created", testFile.exists());
    System.out.println("file size : " + testFile.getUsableSpace());
    assertTrue("File size too small", testFile.getTotalSpace() >= 476208406528l);
    // TODO CHECK IF WE CAN READ AND FIND EXACLTY THE SAME VALUE
//      int count = 0;
//      while (reader.next(key, value)) {
//        assertTrue(count < mapData.size());
//        assertTrue(key == null);
//        final Object obj = value.get(new Text("c_custkey"));
//        final MapWritable expected = mapData.get(((IntWritable) obj).get());
//
//        assertTrue(UtilitiesTestMethods.mapEquals(value, expected));
//        count++;
//      }
//
//      reader.close();

//      assertEquals("Number of lines found and data written don't match", count, mapData.size());


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
  }
}
