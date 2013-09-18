/**
 * Copyright 2012 Twitter, Inc.
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
package parquet.avro;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

public class TestReadWrite {

  @Test
  public void test() throws Exception {
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
        .set("myoptionalarray", genericIntegerArray)
        .set("mymap", ImmutableMap.of("a", 1, "b", 2))
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
    assertEquals(integerArray, nextRecord.get("myoptionalarray"));
    assertEquals(ImmutableMap.of("a", 1, "b", 2), nextRecord.get("mymap"));
    assertEquals(genericFixed, nextRecord.get("myfixed"));
  }
}
