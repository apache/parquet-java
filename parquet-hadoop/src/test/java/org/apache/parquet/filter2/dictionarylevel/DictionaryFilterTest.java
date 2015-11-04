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

package org.apache.parquet.filter2.dictionarylevel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.*;

import java.io.IOException;
import java.util.List;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.filter2.dictionarylevel.DictionaryFilter.canDrop;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by dweeks on 10/19/15.
 */
public class DictionaryFilterTest {

  private static final int nElements = 1000;
  private static final Configuration conf = new Configuration();
  private static  Path file = new Path("target/test/TestDictionaryFilter/testParquetFile");
  private static final MessageType schema = parseMessageType(
      "message test { "
          + "required binary binary_field; "
          + "required int32 int32_field; "
          + "required int64 int64_field; "
          + "required double double_field; "
          + "required float float_field; "
          + "} ");

  private static final int ENGLISH_CHARACTER_NUMBER = 26;
  private static final int[] intValues = new int[] {-100, 302, 3333333, 7654321, 1234567, -2000, -77775, 0, 75, 22223,
      77, 22221, -444443, 205, 12, 44444, 889, 66665, -777889, -7,
      52, 33, -257, 1111, 775, 26};
  private static final long[] longValues = new long[] {-100L, 302L, 3333333L, 7654321L, 1234567L, -2000L, -77775L, 0L,
      75L, 22223L, 77L, 22221L, -444443L, 205L, 12L, 44444L, 889L, 66665L,
      -777889L, -7L, 52L, 33L, -257L, 1111L, 775L, 26L};

  private static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      int index = i % ENGLISH_CHARACTER_NUMBER;
      char c  = (char) ((index) + 'a');
      String b = String.valueOf(c);

      Group group = f.newGroup()
          .append("binary_field", b)
          .append("int32_field", intValues[index])
          .append("int64_field", longValues[index])
          .append("double_field", intValues[index] * 1.0)
          .append("float_field", ((float) (intValues[index] * 2.0)));

      writer.write(group);
    }
    writer.close();
  }

  @BeforeClass
  public static void prepareFile() throws IOException {
    cleanup();

    boolean dictionaryEnabled = true;
    boolean validating = false;
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(
        file,
        new GroupWriteSupport(),
        UNCOMPRESSED, 1024*1024, 1024, 1024*1024,
        dictionaryEnabled, validating, PARQUET_1_0, conf);
    writeData(f, writer);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }


  List<ColumnChunkMetaData> ccmd;
  FSDataInputStream stream;

  @Before
  public void setUp() throws Exception {
    FileSystem fs = FileSystem.getLocal(conf);
    ParquetMetadata meta = ParquetFileReader.readFooter(conf, fs.getFileStatus(file), ParquetMetadataConverter.NO_FILTER);

    ccmd = meta.getBlocks().get(0).getColumns();
    stream = fs.open(file);
  }

  @After
  public void tearDown() throws Exception {
    stream.close();
  }

  @Test
  public void testCanDropEq() throws Exception {
    BinaryColumn b = binaryColumn("binary_field");
    FilterPredicate pred = eq(b, Binary.fromString("c"));

    assertFalse(canDrop(pred, ccmd, stream));
  }

  @Test
  public void testCanDropLt() throws Exception {
    IntColumn i32 = intColumn("int32_field");
    FilterPredicate predDrop = lt(i32, -777889);
    assertTrue(canDrop(predDrop, ccmd, stream));

    FilterPredicate predKeep = lt(i32, -1);
    assertFalse(canDrop(predKeep, ccmd, stream));
  }

  @Test
  public void testCanDropLtEq() throws Exception {
    IntColumn i32 = intColumn("int32_field");
    FilterPredicate pred = ltEq(i32, -777890);

    assertTrue(canDrop(pred, ccmd, stream));
  }

  @Test
  public void testCanDropAnd() throws Exception {
    IntColumn i32 = intColumn("int32_field");
    FilterPredicate pred = and(lt(i32, -777889), gt(i32, 3333332));

    assertTrue(canDrop(pred, ccmd, stream));
  }

}