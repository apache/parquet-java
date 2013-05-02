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
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.mem.MemPageStore;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hive.DeprecatedParquetInputFormat.InputSplitWrapper;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;

/**
 *
 * TestHiveInputFormat
 *
 *
 * @author Mickaël Lacour <m.lacour@criteo.com>
 *
 */
// TODO to understand how to write a file :)
public class TestDeprecatedParquetInputFormat extends TestCase {

  Configuration conf;
  JobConf job;
  FileSystem fs;
  Path dir;
  File testFile;
  Reporter reporter;
  FSDataOutputStream ds;
  Map<Integer, MapWritable> mapData;

  @Override
  protected void setUp() throws Exception {
    //
    // create job and filesystem and reporter and such.
    //
    mapData = new HashMap<Integer, MapWritable>();
    conf = new Configuration();
    job = new JobConf(conf);
    fs = FileSystem.getLocal(conf);
    dir = new Path("testdata/from_java/deprecatedoutputformat/");
    testFile = new File(dir.toString(), "customer");
    reporter = Reporter.NULL;
    if (testFile.exists()) {
      if (!testFile.delete()) {
        throw new RuntimeException("can not remove existing file " + testFile.getAbsolutePath());
      }
    }
    fs.delete(dir, true);
    FileInputFormat.setInputPaths(job, dir);
//      mapData.clear();
//      for (int i = 0; i < 1000; i++) {
//        // yeah same test as pig one :)
//        mapData.put(i, UtilitiesTestMethods.createMap(i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
//                i % 13 == 0 ? null : i, i % 14 == 0 ? null : "phone_" + i, i % 15 == 0 ? null : 1.2d * i, i % 16 == 0 ? null : "mktsegment_" + i,
//                i % 17 == 0 ? null : "comment_" + i));
//      }
    // Write data
    writeFile();

  }

  private void writeFile() throws IOException {
    final MessageType schema = new MessageType("customer",
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "c_custkey"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_name"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_address"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "c_nationkey"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_phone"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.DOUBLE, "c_acctbal"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_mktsegment"),
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_comment"));

    final MemPageStore pageStore = new MemPageStore();
    final ColumnWriteStoreImpl store = new ColumnWriteStoreImpl(pageStore, 8 * 1024);
    //
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

    final RecordConsumer recordWriter = columnIO.getRecordWriter(store);

    int recordCount = 0;
    mapData.clear();
    for (int i = 0; i < 1000; i++) {
      recordWriter.startMessage();
      // yeah same test as pig one :)
      mapData.put(i, UtilitiesTestMethods.createMap(i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
              i % 13 == 0 ? null : i, i % 14 == 0 ? null : "phone_" + i, i % 15 == 0 ? null : 1.2d * i, i % 16 == 0 ? null : "mktsegment_" + i,
              i % 17 == 0 ? null : "comment_" + i));
      saveData(recordWriter, i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
              i % 13 == 0 ? null : i, i % 14 == 0 ? null : "phone_" + i, i % 15 == 0 ? null : 1.2d * i, i % 16 == 0 ? null : "mktsegment_" + i,
              i % 17 == 0 ? null : "comment_" + i);
      recordWriter.endMessage();
      ++recordCount;
    }
    store.flush();

    UtilitiesTestMethods.writeToFile(new Path(testFile.getAbsolutePath()), conf, schema, pageStore, recordCount);
  }

  private void saveData(final RecordConsumer recordWriter, final Integer custkey, final String name, final String address, final Integer nationkey, final String phone,
          final Double acctbal, final String mktsegment, final String comment) {
    UtilitiesTestMethods.writeField(recordWriter, 0, "c_custkey", custkey);
    UtilitiesTestMethods.writeField(recordWriter, 1, "c_name", name);
    UtilitiesTestMethods.writeField(recordWriter, 2, "c_address", address);
    UtilitiesTestMethods.writeField(recordWriter, 3, "c_nationkey", nationkey);
    UtilitiesTestMethods.writeField(recordWriter, 4, "c_phone", phone);
    UtilitiesTestMethods.writeField(recordWriter, 5, "c_acctbal", acctbal);
    UtilitiesTestMethods.writeField(recordWriter, 6, "c_mktsegment", mktsegment);
    UtilitiesTestMethods.writeField(recordWriter, 7, "c_comment", comment);
  }

  public void testParquetHiveInputFormat() throws Exception {

    final ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(testFile.getAbsolutePath()));
    final MessageType schema = readFooter.getFileMetaData().getSchema();

    long size = 0;
    final List<BlockMetaData> blocks = readFooter.getBlocks();
    for (final BlockMetaData block : blocks) {
      size += block.getTotalByteSize();
    }


    final FileInputFormat<Void, MapWritable> format = new DeprecatedParquetInputFormat();
    final String[] locations = new String[]{"localhost"};
    final String schemaToString = schema.toString();
    final ParquetInputSplit realSplit = new ParquetInputSplit(new Path(testFile.getAbsolutePath()), 0, size, locations, blocks,
            schemaToString, schemaToString, readFooter.getFileMetaData().getKeyValueMetaData());

    final DeprecatedParquetInputFormat.InputSplitWrapper splitWrapper = new InputSplitWrapper(realSplit);

    // construct the record reader
    final RecordReader<Void, MapWritable> reader = format.getRecordReader(splitWrapper, job, reporter);

    // create key/value
    final Void key = reader.createKey();
    final MapWritable value = reader.createValue();


    int count = 0;
    while (reader.next(key, value)) {
      assertTrue(count < mapData.size());
      assertTrue(key == null);
      final Object obj = value.get(new Text("c_custkey"));
//        System.out.println("obj : " + obj);
      final MapWritable expected = mapData.get(((IntWritable) obj).get());
//        System.out.println("expected " + expected.entrySet().toString());
//        System.out.println("value " + value.entrySet().toString());

      assertTrue(UtilitiesTestMethods.mapEquals(value, expected));
      count++;
    }

    reader.close();

    assertEquals("Number of lines found and data written don't match", count, mapData.size());
  }
}
