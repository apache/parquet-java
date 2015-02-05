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
package parquet.hadoop;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.column.Encoding.DELTA_BYTE_ARRAY;
import static parquet.column.Encoding.PLAIN;
import static parquet.column.Encoding.PLAIN_DICTIONARY;
import static parquet.column.Encoding.RLE_DICTIONARY;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static parquet.hadoop.ParquetFileReader.readFooter;
import static parquet.hadoop.TestUtils.enforceEmptyDir;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static parquet.schema.MessageTypeParser.parseMessageType;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.column.Encoding;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.schema.MessageType;

public class TestParquetWriter {

  @Test
  public void test() throws Exception {
    Configuration conf = new Configuration();
    Path root = new Path("target/tests/TestParquetWriter/");
    enforceEmptyDir(conf, root);
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
    Map<String, Encoding> expected = new HashMap<String, Encoding>();
    expected.put("10-" + PARQUET_1_0, PLAIN_DICTIONARY);
    expected.put("1000-" + PARQUET_1_0, PLAIN);
    expected.put("10-" + PARQUET_2_0, RLE_DICTIONARY);
    expected.put("1000-" + PARQUET_2_0, DELTA_BYTE_ARRAY);
    for (int modulo : asList(10, 1000)) {
      for (WriterVersion version : WriterVersion.values()) {
        Path file = new Path(root, version.name() + "_" + modulo);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
            file,
            new GroupWriteSupport(),
            UNCOMPRESSED, 1024, 1024, 512, true, false, version, conf);
        for (int i = 0; i < 1000; i++) {
          writer.write(
              f.newGroup()
              .append("binary_field", "test" + (i % modulo))
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
          assertEquals("test" + (i % modulo), group.getBinary("binary_field", 0).toStringUsingUTF8());
          assertEquals(32, group.getInteger("int32_field", 0));
          assertEquals(64l, group.getLong("int64_field", 0));
          assertEquals(true, group.getBoolean("boolean_field", 0));
          assertEquals(1.0f, group.getFloat("float_field", 0), 0.001);
          assertEquals(2.0d, group.getDouble("double_field", 0), 0.001);
          assertEquals("foo", group.getBinary("flba_field", 0).toStringUsingUTF8());
          assertEquals(Binary.fromByteArray(new byte[12]), group.getInt96("int96_field", 0));
        }
        reader.close();
        ParquetMetadata footer = readFooter(conf, file, NO_FILTER);
        for (BlockMetaData blockMetaData : footer.getBlocks()) {
          for (ColumnChunkMetaData column : blockMetaData.getColumns()) {
            if (column.getPath().toDotString().equals("binary_field")) {
              String key = modulo + "-" + version;
              Encoding expectedEncoding = expected.get(key);
              assertTrue(
                  key + ":" + column.getEncodings() + " should contain " + expectedEncoding,
                  column.getEncodings().contains(expectedEncoding));
            }
          }
        }
      }
    }
  }
}
