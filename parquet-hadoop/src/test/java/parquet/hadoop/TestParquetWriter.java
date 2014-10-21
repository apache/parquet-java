package parquet.hadoop;

import static org.junit.Assert.assertEquals;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static parquet.schema.MessageTypeParser.parseMessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.column.ParquetProperties.WriterVersion;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.io.api.Binary;
import parquet.schema.MessageType;

public class TestParquetWriter {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestParquetWriter/");
    FileSystem fs = root.getFileSystem(conf);
    if (fs.exists(root)) {
      fs.delete(root, true);
    }
    fs.mkdirs(root);
    MessageType schema = parseMessageType(
        "message test { "
        + "required binary binary_field; "
        + "required int32 int32_field; "
        + "required int64 int64_field; "
        + "required boolean boolean_field; "
        + "required float float_field; "
        + "required double double_field; "
        + "required fixed_len_byte_array(3) flba_field; "
        + "required int96 int96_field; "
        + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    for (WriterVersion version : WriterVersion.values()) {
      Path file = new Path(root, version.name());
      ParquetWriter<Group> writer = new ParquetWriter<Group>(
          file,
          new GroupWriteSupport(),
          UNCOMPRESSED, 1024, 1024, 512, true, false, version, conf);
      for (int i = 0; i < 1000; i++) {
        writer.write(
            f.newGroup()
            .append("binary_field", "test")
            .append("int32_field", 32)
            .append("int64_field", 64l)
            .append("boolean_field", true)
            .append("float_field", 1.0f)
            .append("double_field", 2.0d)
            .append("flba_field", "foo")
            .append("int96_field", Binary.fromByteArray(new byte[12])));
      }
      writer.close();
      ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
      for (int i = 0; i < 1000; i++) {
        Group group = reader.read();
        assertEquals("test", group.getBinary("binary_field", 0).toStringUsingUTF8());
        assertEquals(32, group.getInteger("int32_field", 0));
        assertEquals(64l, group.getLong("int64_field", 0));
        assertEquals(true, group.getBoolean("boolean_field", 0));
        assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
        assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
        assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
        assertEquals(Binary.fromByteArray(new byte[12]), group.getInt96("int96_field", 0));
      }
      reader.close();
    }

  }
}
