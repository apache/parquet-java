package parquet.avro;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestReadWrite {

  @Test
  public void test() throws Exception {
    Schema schema = new Schema.Parser().parse(
        Resources.getResource("all-minus-fixed.avsc").openStream());

    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path file = new Path(tmp.getPath());

    AvroParquetWriter<GenericRecord> writer = new
        AvroParquetWriter<GenericRecord>(file, schema);

    GenericData.Record nestedRecord = new GenericRecordBuilder(
          schema.getField("mynestedrecord").schema())
        .set("mynestedint", 1).build();

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
        .set("myarray", new GenericData.Array(Schema.createArray(Schema.create(Schema.Type.INT)),
            Arrays.asList(1, 2)))
        .set("mymap", ImmutableMap.of("a", 1, "b", 2))
        // TODO: support fixed encoding by plumbing in FIXED_LEN_BYTE_ARRAY
        //.set("myfixed", new GenericData.Fixed(Schema.createFixed("ignored", null, null, 1),
        //    new byte[] { (byte) 65 }))
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
    // TODO: fix nested records. AvroGenericRecordConverter.start() is not being called
    //assertEquals(nestedRecord, nextRecord.get("mynestedrecord"));
    assertEquals(Arrays.asList(1, 2), nextRecord.get("myarray"));
    assertEquals(ImmutableMap.of("a", 1, "b", 2), nextRecord.get("mymap"));
    //assertEquals(new byte[] { (byte) 65 }, nextRecord.get("myfixed"));
  }
}
