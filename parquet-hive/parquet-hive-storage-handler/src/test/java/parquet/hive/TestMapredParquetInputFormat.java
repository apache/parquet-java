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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;

import parquet.column.ParquetProperties.WriterVersion;
import parquet.column.impl.ColumnWriteStoreImpl;
import parquet.column.page.mem.MemPageStore;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hive.MapredParquetInputFormat.InputSplitWrapper;
import parquet.hive.read.DataWritableReadSupport;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.OriginalType;
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
public class TestMapredParquetInputFormat {

  private Configuration conf;
  private JobConf job;
  private FileSystem fs;
  private Path dir;
  private File testFile;
  private Reporter reporter;
  private Map<Integer, ArrayWritable> mapData;

  @Test
  public void testParquetHiveInputFormatWithoutSpecificSchema() throws Exception {
    final String schemaRequested = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_name;\n"
            + "  optional binary c_address;\n"
            + "  optional int32 c_nationkey;\n"
            + "  optional binary c_phone;\n"
            + "  optional double c_acctbal;\n"
            + "  optional binary c_mktsegment;\n"
            + "  optional binary c_comment;\n"
            + "  optional group c_map (MAP_KEY_VALUE) {\n"
            + "    repeated group map {\n"
            + "      required binary key;\n"
            + "      optional binary value;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group c_list (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element;\n"
            + "    }\n"
            + "  }\n"
            + "}";
    readParquetHiveInputFormat(schemaRequested, new Integer[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
  }

  @Test
  public void testParquetHiveInputFormatWithSpecificSchema() throws Exception {
    final String schemaRequested = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_name;\n"
            + "  optional double c_acctbal;\n"
            + "  optional binary c_mktsegment;\n"
            + "  optional binary c_comment;\n"
            + "}";
    readParquetHiveInputFormat(schemaRequested, new Integer[] {0, 1, 5, 6, 7});
  }

  @Test
  public void testParquetHiveInputFormatWithSpecificSchemaRandomColumn() throws Exception {
    final String schemaRequested = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_mktsegment;\n"
            + "}";
    readParquetHiveInputFormat(schemaRequested, new Integer[] {0, 6});
  }

  @Test
  public void testParquetHiveInputFormatWithSpecificSchemaFirstColumn() throws Exception {
    final String schemaRequested = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "}";
    readParquetHiveInputFormat(schemaRequested, new Integer[] {0});
  }

  @Test
  public void testParquetHiveInputFormatWithSpecificSchemaUnknownColumn() throws Exception {
    final String schemaRequested = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional int32 unknown;\n"
            + "}";
    readParquetHiveInputFormat(schemaRequested, new Integer[] {0, Integer.MIN_VALUE});
  }

  @Test
  public void testGetSplit() throws Exception {
    final ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(testFile.getAbsolutePath()));

    final MessageType fileSchema = readFooter.getFileMetaData().getSchema();
    final MessageType requestedSchema = MessageTypeParser.parseMessageType("message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_name;\n"
            + "  optional double c_acctbal;\n"
            + "  optional binary c_mktsegment;\n"
            + "  optional binary c_comment;\n"
            + "}");
    final MessageType hiveSchema = MessageTypeParser.parseMessageType("message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_name;\n"
            + "  optional binary c_address;\n"
            + "  optional int32 c_nationkey;\n"
            + "  optional binary c_phone;\n"
            + "  optional double c_acctbal;\n"
            + "  optional binary c_mktsegment;\n"
            + "  optional binary c_comment;\n"
            + "  optional group c_map (MAP_KEY_VALUE) {\n"
            + "    repeated group map {\n"
            + "      required binary key;\n"
            + "      optional binary value;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group c_list (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element;\n"
            + "    }\n"
            + "  }\n"
            + "  optional binary unknown;\n"
            + "}");

    // Put columns and projection info in the conf
    List<String> columns = new ArrayList<String>();
    List<Integer> readColumns = new ArrayList<Integer>();
    for (int i = 0; i < hiveSchema.getFieldCount(); ++i) {
      final String name = hiveSchema.getType(i).getName();
      columns.add(name);
      if (requestedSchema.containsField(name)) {
        readColumns.add(i);
      }
    }
    job.set("columns", StringUtils.join(columns, ","));
    ColumnProjectionUtils.setReadColumnIDs(job, readColumns);

    long size = 0;
    final List<BlockMetaData> blocks = readFooter.getBlocks();
    for (final BlockMetaData block : blocks) {
      size += block.getTotalByteSize();
    }

    final FileInputFormat<Void, ArrayWritable> format = new MapredParquetInputFormat();
    final String[] locations = new String[] {"localhost"};

    final Map<String, String> readSupportMetaData = new HashMap<String, String>();
    readSupportMetaData.put(DataWritableReadSupport.HIVE_SCHEMA_KEY, hiveSchema.toString());
    final ParquetInputSplit realSplit = new ParquetInputSplit(new Path(testFile.getAbsolutePath()), 0, size, locations, blocks,
            fileSchema.toString(), requestedSchema.toString(), readFooter.getFileMetaData().getKeyValueMetaData(), readSupportMetaData);

    final MapredParquetInputFormat.InputSplitWrapper splitWrapper = new InputSplitWrapper(realSplit);

    // construct the record reader
    final RecordReader<Void, ArrayWritable> reader = format.getRecordReader(splitWrapper, job, reporter);

    assertEquals("Wrong real split inside wrapper", realSplit,
            ((MapredParquetInputFormat.RecordReaderWrapper) reader).getSplit(splitWrapper, job));

    // Recreate the split using getSplit, as Hive would
    final FileSplit fileSplit = new FileSplit(splitWrapper.getPath(), splitWrapper.getStart(), splitWrapper.getLength(), splitWrapper.getLocations());
    final ParquetInputSplit recreatedSplit = ((MapredParquetInputFormat.RecordReaderWrapper) reader).getSplit(fileSplit, job);
    assertTrue("Wrong file schema", UtilitiesTestMethods.smartCheckSchema(fileSchema,
            MessageTypeParser.parseMessageType(recreatedSplit.getFileSchema())));
    assertTrue("Wrong requested schema", UtilitiesTestMethods.smartCheckSchema(requestedSchema,
            MessageTypeParser.parseMessageType(recreatedSplit.getRequestedSchema())));
    assertTrue("Wrong hive schema", UtilitiesTestMethods.smartCheckSchema(hiveSchema,
            MessageTypeParser.parseMessageType(recreatedSplit.getReadSupportMetadata().get(DataWritableReadSupport.HIVE_SCHEMA_KEY))));
  }

  @Before
  public void setUp() throws Exception {
    //
    // create job and filesystem and reporter and such.
    //
    mapData = new HashMap<Integer, ArrayWritable>();
    conf = new Configuration();
    job = new JobConf(conf);
    fs = FileSystem.getLocal(conf);
    dir = new Path("target/tests/from_java/deprecatedoutputformat/");
    testFile = new File(dir.toString(), "customer");
    reporter = Reporter.NULL;
    if (testFile.exists()) {
      if (!testFile.delete()) {
        throw new RuntimeException("can not remove existing file " + testFile.getAbsolutePath());
      }
    }
    fs.delete(dir, true);
    FileInputFormat.setInputPaths(job, dir);

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
            new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "c_comment"),
            new GroupType(Repetition.OPTIONAL, "c_map", OriginalType.MAP_KEY_VALUE, new GroupType(Repetition.REPEATED, "map", new PrimitiveType(Repetition.REQUIRED, PrimitiveTypeName.BINARY, "key"), new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "value"))),
            new GroupType(Repetition.OPTIONAL, "c_list", OriginalType.LIST, new GroupType(Repetition.REPEATED, "bag", new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "array_element"))));

    final MemPageStore pageStore = new MemPageStore(1000);
    final ColumnWriteStoreImpl store = new ColumnWriteStoreImpl(pageStore, 8 * 1024, 8 * 1024, 8 * 1024, false, WriterVersion.PARQUET_1_0);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

    final RecordConsumer recordWriter = columnIO.getRecordWriter(store);

    Map<String, String> map = new HashMap<String, String>();
    map.put("testkey", "testvalue");
    map.put("foo", "bar");

    List<Integer> list = new ArrayList<Integer>();
    list.add(0);
    list.add(12);
    list.add(17);

    int recordCount = 0;
    mapData.clear();
    for (int i = 0; i < 1000; i++) {
      recordWriter.startMessage();
      mapData.put(i, UtilitiesTestMethods.createArrayWritable(i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
              i % 13 == 0 ? null : i, i % 14 == 0 ? null : "phone_" + i, i % 15 == 0 ? null : 1.2d * i, i % 16 == 0 ? null : "mktsegment_" + i,
              i % 17 == 0 ? null : "comment_" + i, i % 18 == 0 ? null : map, i % 19 == 0 ? null : list));
      saveData(recordWriter, i, i % 11 == 0 ? null : "name_" + i, i % 12 == 0 ? null : "add_" + i,
              i % 13 == 0 ? null : i, i % 14 == 0 ? null : "phone_" + i, i % 15 == 0 ? null : 1.2d * i, i % 16 == 0 ? null : "mktsegment_" + i,
              i % 17 == 0 ? null : "comment_" + i, i % 18 == 0 ? null : map, i % 19 == 0 ? null : list);
      recordWriter.endMessage();
      ++recordCount;
    }
    store.flush();

    UtilitiesTestMethods.writeToFile(new Path(testFile.getAbsolutePath()), conf, schema, pageStore, recordCount);
  }

  private void saveData(final RecordConsumer recordWriter, final Integer custkey, final String name, final String address, final Integer nationkey, final String phone,
          final Double acctbal, final String mktsegment, final String comment, final Map<String, String> map, final List<Integer> list) {
    UtilitiesTestMethods.writeField(recordWriter, 0, "c_custkey", custkey);
    UtilitiesTestMethods.writeField(recordWriter, 1, "c_name", name);
    UtilitiesTestMethods.writeField(recordWriter, 2, "c_address", address);
    UtilitiesTestMethods.writeField(recordWriter, 3, "c_nationkey", nationkey);
    UtilitiesTestMethods.writeField(recordWriter, 4, "c_phone", phone);
    UtilitiesTestMethods.writeField(recordWriter, 5, "c_acctbal", acctbal);
    UtilitiesTestMethods.writeField(recordWriter, 6, "c_mktsegment", mktsegment);
    UtilitiesTestMethods.writeField(recordWriter, 7, "c_comment", comment);
    UtilitiesTestMethods.writeField(recordWriter, 8, "c_map", map);
    UtilitiesTestMethods.writeField(recordWriter, 9, "c_list", list);
  }

  private void readParquetHiveInputFormat(final String schemaRequested, final Integer[] arrCheckIndexValues) throws Exception {
    final ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, new Path(testFile.getAbsolutePath()));
    final MessageType schema = readFooter.getFileMetaData().getSchema();

    long size = 0;
    final List<BlockMetaData> blocks = readFooter.getBlocks();
    for (final BlockMetaData block : blocks) {
      size += block.getTotalByteSize();
    }

    final FileInputFormat<Void, ArrayWritable> format = new MapredParquetInputFormat();
    final String[] locations = new String[] {"localhost"};
    final String schemaToString = schema.toString();
    System.out.println(schemaToString);

    final String specificSchema = schemaRequested == null ? schemaToString : schemaRequested;

    // Set the configuration parameters
    final String columnsStr = "message customer {\n"
            + "  optional int32 c_custkey;\n"
            + "  optional binary c_name;\n"
            + "  optional binary c_address;\n"
            + "  optional int32 c_nationkey;\n"
            + "  optional binary c_phone;\n"
            + "  optional double c_acctbal;\n"
            + "  optional binary c_mktsegment;\n"
            + "  optional binary c_comment;\n"
            + "  optional group c_map (MAP_KEY_VALUE) {\n"
            + "    repeated group map {\n"
            + "      required binary key;\n"
            + "      optional binary value;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group c_list (LIST) {\n"
            + "    repeated group bag {\n"
            + "      optional int32 array_element;\n"
            + "    }\n"
            + "  }\n"
            + "  optional int32 unknown;\n"
            + "}";


    final Map<String, String> readSupportMetaData = new HashMap<String, String>();
    readSupportMetaData.put(DataWritableReadSupport.HIVE_SCHEMA_KEY, columnsStr);
    final ParquetInputSplit realSplit = new ParquetInputSplit(new Path(testFile.getAbsolutePath()), 0, size, locations, blocks,
            schemaToString, specificSchema, readFooter.getFileMetaData().getKeyValueMetaData(), readSupportMetaData);

    final MapredParquetInputFormat.InputSplitWrapper splitWrapper = new InputSplitWrapper(realSplit);

    // construct the record reader
    final RecordReader<Void, ArrayWritable> reader = format.getRecordReader(splitWrapper, job, reporter);

    // create key/value
    final Void key = reader.createKey();
    final ArrayWritable value = reader.createValue();

    int count = 0;
    final int sizeExpected = mapData.size();
    while (reader.next(key, value)) {
      assertTrue(count < sizeExpected);
      assertTrue(key == null);
      final Writable[] arrValue = value.get();
      final ArrayWritable expected = mapData.get(((IntWritable) arrValue[0]).get());
      final Writable[] arrExpected = expected.get();
      assertEquals(arrValue.length, arrExpected.length);

      final boolean deepEquals = UtilitiesTestMethods.smartCheckArray(arrValue, arrExpected, arrCheckIndexValues);

      assertTrue(deepEquals);
      count++;
    }
    System.out.println("nb lines " + count);
    reader.close();

    assertEquals("Number of lines found and data written don't match", count, sizeExpected);
  }
}
