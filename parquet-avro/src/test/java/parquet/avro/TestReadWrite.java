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
package parquet.avro;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.node.NullNode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

public class TestReadWrite {

  @Test
  public void testEmptyArray() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("array.avsc").openStream());

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());

    AvroParquetWriter<GenericRecord> writer = 
        new AvroParquetWriter<GenericRecord>(file, schema);

    // Write a record with an empty array.
    List<Integer> emptyArray = new ArrayList<Integer>();
    GenericData.Record record = new GenericRecordBuilder(schema)
        .set("myarray", emptyArray).build();
    writer.write(record);
    writer.close();

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(file);
    GenericRecord nextRecord = reader.read();

    assertNotNull(nextRecord);
    assertEquals(emptyArray, nextRecord.get("myarray"));
  }

  @Test
  public void testEmptyMap() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("map.avsc").openStream());

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());

    AvroParquetWriter<GenericRecord> writer = 
        new AvroParquetWriter<GenericRecord>(file, schema);

    // Write a record with an empty map.
    ImmutableMap emptyMap = new ImmutableMap.Builder<String, Integer>().build();
    GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", emptyMap).build();
    writer.write(record);
    writer.close();

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(file);
    GenericRecord nextRecord = reader.read();

    assertNotNull(nextRecord);
    assertEquals(emptyMap, nextRecord.get("mymap"));
  }

  @Test
  public void testMapWithUtf8Key() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("map.avsc").openStream());

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());

    AvroParquetWriter<GenericRecord> writer = 
        new AvroParquetWriter<GenericRecord>(file, schema);

    // Write a record with a map with Utf8 keys.
    GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mymap", ImmutableMap.of(new Utf8("a"), 1, new Utf8("b"), 2))
        .build();
    writer.write(record);
    writer.close();

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(file);
    GenericRecord nextRecord = reader.read();

    assertNotNull(nextRecord);
    assertEquals(ImmutableMap.of("a", 1, "b", 2), nextRecord.get("mymap"));
  }

  @Test
  public void testAll() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all.avsc").openStream());

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());
    
    AvroParquetWriter<GenericRecord> writer = new
        AvroParquetWriter<GenericRecord>(file, schema);

    GenericData.Record nestedRecord = new GenericRecordBuilder(
        schema.getField("mynestedrecord").schema())
            .set("mynestedint", 1).build();

    List<Integer> integerArray = Arrays.asList(1, 2, 3);
    GenericData.Array<Integer> genericIntegerArray = new GenericData.Array<Integer>(
        Schema.createArray(Schema.create(Schema.Type.INT)), integerArray);

    GenericFixed genericFixed = new GenericData.Fixed(
        Schema.createFixed("fixed", null, null, 1), new byte[] { (byte) 65 });

    List<Integer> emptyArray = new ArrayList<Integer>();
    ImmutableMap emptyMap = new ImmutableMap.Builder<String, Integer>().build();

    GenericData.Record record = new GenericRecordBuilder(schema)
        .set("mynull", null)
        .set("myboolean", true)
        .set("myint", 1)
        .set("mylong", 2L)
        .set("myfloat", 3.1f)
        .set("mydouble", 4.1)
        .set("mybytes", ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)))
        .set("mystring", "hello")
        .set("mynestedrecord", nestedRecord)
        .set("myenum", "a")
        .set("myarray", genericIntegerArray)
        .set("myemptyarray", emptyArray)
        .set("myoptionalarray", genericIntegerArray)
        .set("mymap", ImmutableMap.of("a", 1, "b", 2))
        .set("myemptymap", emptyMap)
        .set("myfixed", genericFixed)
        .build();

    writer.write(record);
    writer.close();

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(file);
    GenericRecord nextRecord = reader.read();

    assertNotNull(nextRecord);
    assertEquals(null, nextRecord.get("mynull"));
    assertEquals(true, nextRecord.get("myboolean"));
    assertEquals(1, nextRecord.get("myint"));
    assertEquals(2L, nextRecord.get("mylong"));
    assertEquals(3.1f, nextRecord.get("myfloat"));
    assertEquals(4.1, nextRecord.get("mydouble"));
    assertEquals(ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)), nextRecord.get("mybytes"));
    assertEquals("hello", nextRecord.get("mystring"));
    assertEquals("a", nextRecord.get("myenum"));
    assertEquals(nestedRecord, nextRecord.get("mynestedrecord"));
    assertEquals(integerArray, nextRecord.get("myarray"));
    assertEquals(emptyArray, nextRecord.get("myemptyarray"));
    assertEquals(integerArray, nextRecord.get("myoptionalarray"));
    assertEquals(ImmutableMap.of("a", 1, "b", 2), nextRecord.get("mymap"));
    assertEquals(emptyMap, nextRecord.get("myemptymap"));
    assertEquals(genericFixed, nextRecord.get("myfixed"));
  }

  @Test
  public void testAllUsingDefaultAvroSchema() throws Exception {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());

    // write file using Parquet APIs
    ParquetWriter<Map<String, Object>> parquetWriter = new ParquetWriter<Map<String, Object>>(file,
        new WriteSupport<Map<String, Object>>() {

      private RecordConsumer recordConsumer;

      @Override
      public WriteContext init(Configuration configuration) {
        return new WriteContext(MessageTypeParser.parseMessageType(TestAvroSchemaConverter.ALL_PARQUET_SCHEMA),
            new HashMap<String, String>());
      }

      @Override
      public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
      }

      @Override
      public void write(Map<String, Object> record) {
        recordConsumer.startMessage();

        int index = 0;

        recordConsumer.startField("myboolean", index);
        recordConsumer.addBoolean((Boolean) record.get("myboolean"));
        recordConsumer.endField("myboolean", index++);

        recordConsumer.startField("myint", index);
        recordConsumer.addInteger((Integer) record.get("myint"));
        recordConsumer.endField("myint", index++);

        recordConsumer.startField("mylong", index);
        recordConsumer.addLong((Long) record.get("mylong"));
        recordConsumer.endField("mylong", index++);

        recordConsumer.startField("myfloat", index);
        recordConsumer.addFloat((Float) record.get("myfloat"));
        recordConsumer.endField("myfloat", index++);

        recordConsumer.startField("mydouble", index);
        recordConsumer.addDouble((Double) record.get("mydouble"));
        recordConsumer.endField("mydouble", index++);

        recordConsumer.startField("mybytes", index);
        recordConsumer.addBinary(Binary.fromByteBuffer((ByteBuffer) record.get("mybytes")));
        recordConsumer.endField("mybytes", index++);

        recordConsumer.startField("mystring", index);
        recordConsumer.addBinary(Binary.fromString((String) record.get("mystring")));
        recordConsumer.endField("mystring", index++);

        recordConsumer.startField("mynestedrecord", index);
        recordConsumer.startGroup();
        recordConsumer.startField("mynestedint", 0);
        recordConsumer.addInteger((Integer) record.get("mynestedint"));
        recordConsumer.endField("mynestedint", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("mynestedrecord", index++);

        recordConsumer.startField("myenum", index);
        recordConsumer.addBinary(Binary.fromString((String) record.get("myenum")));
        recordConsumer.endField("myenum", index++);

        recordConsumer.startField("myarray", index);
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        for (int val : (int[]) record.get("myarray")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myarray", index++);

        recordConsumer.startField("myoptionalarray", index);
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        for (int val : (int[]) record.get("myoptionalarray")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myoptionalarray", index++);

        recordConsumer.startField("myrecordarray", index);
        recordConsumer.startGroup();
        recordConsumer.startField("array", 0);
        recordConsumer.startGroup();
        recordConsumer.startField("a", 0);
        for (int val : (int[]) record.get("myrecordarraya")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("a", 0);
        recordConsumer.startField("b", 1);
        for (int val : (int[]) record.get("myrecordarrayb")) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("b", 1);
        recordConsumer.endGroup();
        recordConsumer.endField("array", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("myrecordarray", index++);

        recordConsumer.startField("mymap", index);
        recordConsumer.startGroup();
        recordConsumer.startField("map", 0);
        recordConsumer.startGroup();
        Map<String, Integer> mymap = (Map<String, Integer>) record.get("mymap");
        recordConsumer.startField("key", 0);
        for (String key : mymap.keySet()) {
          recordConsumer.addBinary(Binary.fromString(key));
        }
        recordConsumer.endField("key", 0);
        recordConsumer.startField("value", 1);
        for (int val : mymap.values()) {
          recordConsumer.addInteger(val);
        }
        recordConsumer.endField("value", 1);
        recordConsumer.endGroup();
        recordConsumer.endField("map", 0);
        recordConsumer.endGroup();
        recordConsumer.endField("mymap", index++);

        recordConsumer.startField("myfixed", index);
        recordConsumer.addBinary(Binary.fromByteArray((byte[]) record.get("myfixed")));
        recordConsumer.endField("myfixed", index++);

        recordConsumer.endMessage();
      }
    });
    Map<String, Object> record = new HashMap<String, Object>();
    record.put("myboolean", true);
    record.put("myint", 1);
    record.put("mylong", 2L);
    record.put("myfloat", 3.1f);
    record.put("mydouble", 4.1);
    record.put("mybytes", ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)));
    record.put("mystring", "hello");
    record.put("myenum", "a");
    record.put("mynestedint", 1);
    record.put("myarray", new int[] {1, 2, 3});
    record.put("myoptionalarray", new int[]{1, 2, 3});
    record.put("myrecordarraya", new int[] {1, 2, 3});
    record.put("myrecordarrayb", new int[] {4, 5, 6});
    record.put("mymap", ImmutableMap.of("a", 1, "b", 2));
    record.put("myfixed", new byte[] { (byte) 65 });
    parquetWriter.write(record);
    parquetWriter.close();

    Schema nestedRecordSchema = Schema.createRecord("mynestedrecord", null, null, false);
    nestedRecordSchema.setFields(Arrays.asList(
        new Schema.Field("mynestedint", Schema.create(Schema.Type.INT), null, null)
    ));
    GenericData.Record nestedRecord = new GenericRecordBuilder(nestedRecordSchema)
        .set("mynestedint", 1).build();

    List<Integer> integerArray = Arrays.asList(1, 2, 3);

    Schema recordArraySchema = Schema.createRecord("array", null, null, false);
    recordArraySchema.setFields(Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("b", Schema.create(Schema.Type.INT), null, null)
    ));
    GenericRecordBuilder builder = new GenericRecordBuilder(recordArraySchema);
    List<GenericData.Record> recordArray = new ArrayList<GenericData.Record>();
    recordArray.add(builder.set("a", 1).set("b", 4).build());
    recordArray.add(builder.set("a", 2).set("b", 5).build());
    recordArray.add(builder.set("a", 3).set("b", 6).build());
    GenericData.Array<GenericData.Record> genericRecordArray = new GenericData.Array<GenericData.Record>(
        Schema.createArray(recordArraySchema), recordArray);

    GenericFixed genericFixed = new GenericData.Fixed(
        Schema.createFixed("fixed", null, null, 1), new byte[] { (byte) 65 });

    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(file);
    GenericRecord nextRecord = reader.read();
    assertNotNull(nextRecord);
    assertEquals(true, nextRecord.get("myboolean"));
    assertEquals(1, nextRecord.get("myint"));
    assertEquals(2L, nextRecord.get("mylong"));
    assertEquals(3.1f, nextRecord.get("myfloat"));
    assertEquals(4.1, nextRecord.get("mydouble"));
    assertEquals(ByteBuffer.wrap("hello".getBytes(Charsets.UTF_8)), nextRecord.get("mybytes"));
    assertEquals("hello", nextRecord.get("mystring"));
    assertEquals("a", nextRecord.get("myenum"));
    assertEquals(nestedRecord, nextRecord.get("mynestedrecord"));
    assertEquals(integerArray, nextRecord.get("myarray"));
    assertEquals(integerArray, nextRecord.get("myoptionalarray"));
    assertEquals(genericRecordArray, nextRecord.get("myrecordarray"));
    assertEquals(ImmutableMap.of("a", 1, "b", 2), nextRecord.get("mymap"));
    assertEquals(genericFixed, nextRecord.get("myfixed"));

  }

  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  /** FIXME test fails!
   * potentially different underlying behavior than ParquetAvroInputFormat/ParquetInputFormat
   * is it attempting to merge the parquet schemas as oppossed to just merging the avro ones?
   * i would think:
   *  read parquet file 1, extract avro record with schema 1 and migrate to schema 2
   *  read parquet file 2, extract avro record with schema 2
   * but potentially parquet schemas in all files are merged and the same logic is used
   * for to extract all records, which isn't necessarily sound schema evolution...
   */
  @Test
  public void testSchemaCompatibility() throws Exception {
    // write to a file with schema 1
    Schema schema1 = new Schema.Parser().parse(Resources.getResource("foo1.avsc").openStream());
    File file1 = dir.newFile("foo1.avro.parquet");
    Path path1 = new Path(file1.getPath());
    file1.delete();
    AvroParquetWriter<GenericRecord> writer1 = new AvroParquetWriter<GenericRecord>(path1, schema1);
    GenericData.Record record1 = new GenericRecordBuilder(schema1)
        .set("field_1", new Utf8("a"))
      .build();
    writer1.write(record1);
    writer1.close();

    // write to a different file with schema 2
    Schema schema2 = new Schema.Parser().parse(Resources.getResource("foo2.avsc").openStream());
    File file2 = dir.newFile("foo2.avro.parquet");
    Path path2 = new Path(file2.getPath());
    file2.delete();
    AvroParquetWriter<GenericRecord> writer2 = new AvroParquetWriter<GenericRecord>(path2, schema2);
    GenericData.Record record2 = new GenericRecordBuilder(schema2)
        .set("field_1", new Utf8("b"))
        .build();
    writer2.write(record2);
    writer2.close();

    // read from both files, testing schema compatibility between schema1 and schema2
    Path parent = new Path(dir.getRoot().getPath());
    Configuration configuration = new Configuration(false);
    AvroReadSupport.setAvroReadSchema(configuration, schema2);
    AvroReadSupport.enableAvroSchemaCompatibilityCheck(configuration);
    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(configuration, parent);
    GenericRecord nextRecord1 = reader.read();
    assertNotNull(nextRecord1);
    assertEquals(nextRecord1.get("field_1"), "a");
    GenericRecord nextRecord2 = reader.read();
    assertNotNull(nextRecord2);
    assertEquals(nextRecord2.get("field_1"), "b");
  }

}
