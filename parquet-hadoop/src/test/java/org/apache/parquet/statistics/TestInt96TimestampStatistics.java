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
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.TypeDefinedOrder;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for INT96 timestamp statistics support (INT96_TIMESTAMP_ORDER).
 */
public class TestInt96TimestampStatistics {

  private static final MessageType SCHEMA =
      parseMessageType("message test { required int96 ts; required int64 id; } ");

  // Chronologically: EARLY < SAME_DAY_EARLY < LATE_IN_DAY < NEXT_DAY.
  // Byte-wise lexicographic comparison would order these incorrectly (nanos bytes come first),
  // so these values detect a reader/writer using the wrong order.
  private static final Binary EARLY = int96(2440000, 123L); // 1968-05-23 00:00:00.000000123
  private static final Binary SAME_DAY_EARLY = int96(2440588, 1_000L); // 1970-01-01 00:00:00.000001
  private static final Binary LATE_IN_DAY = int96(2440588, 86_399_999_999_999L); // 1970-01-01 23:59:59.999...
  private static final Binary NEXT_DAY = int96(2440589, 0L); // 1970-01-02 00:00:00

  private static final List<Binary> VALUES = List.of(LATE_IN_DAY, NEXT_DAY, EARLY, SAME_DAY_EARLY);
  private static final Binary EXPECTED_MIN = EARLY;
  private static final Binary EXPECTED_MAX = NEXT_DAY;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private static Binary int96(int julianDay, long nanosOfDay) {
    return Binary.fromConstantByteArray(ByteBuffer.allocate(12)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putLong(nanosOfDay)
        .putInt(julianDay)
        .array());
  }

  private File writeFile() throws IOException {
    File file = new File(tmp.getRoot(), "int96.parquet");
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
        .withConf(conf)
        .build();
    try {
      for (int i = 0; i < VALUES.size(); i++) {
        writer.write(factory.newGroup().append("ts", VALUES.get(i)).append("id", (long) i));
      }
    } finally {
      writer.close();
    }
    return file;
  }

  private static ParquetMetadata readFooter(File file, Configuration conf) throws IOException {
    Path path = new Path(file.getAbsolutePath());
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(path, conf),
        HadoopReadOptions.builder(conf, path).build())) {
      return reader.getFooter();
    }
  }

  private static FileMetaData readRawFooter(File file) throws IOException {
    byte[] bytes = Files.readAllBytes(file.toPath());
    int footerLen = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .getInt();
    int footerStart = bytes.length - 8 - footerLen;
    return Util.readFileMetaData(new ByteArrayInputStream(bytes, footerStart, footerLen));
  }

  /** Rewrites the footer of src into a new file, keeping all data pages byte-identical. */
  private File rewriteFooter(File src, FileMetaData footer, String name)
      throws IOException {
    byte[] bytes = Files.readAllBytes(src.toPath());
    int footerLen = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .getInt();
    int footerStart = bytes.length - 8 - footerLen;
    File dst = new File(tmp.getRoot(), name);
    try (FileOutputStream out = new FileOutputStream(dst)) {
      out.write(bytes, 0, footerStart);
      ByteArrayOutputStream serialized = new ByteArrayOutputStream();
      Util.writeFileMetaData(footer, serialized);
      out.write(serialized.toByteArray());
      out.write(ByteBuffer.allocate(4)
          .order(ByteOrder.LITTLE_ENDIAN)
          .putInt(serialized.size())
          .array());
      out.write(ParquetFileWriter.MAGIC);
    }
    return dst;
  }

  /**
   * Retags the INT96 column orders in the footer as TYPE_ORDER while leaving the (already written)
   * min/max statistics in place. This simulates a legacy writer that emitted INT96 stats under
   * TYPE_ORDER before INT96_TIMESTAMP_ORDER existed; such stats must be ignored by the reader.
   */
  private static void downgradeInt96ToTypeOrder(FileMetaData footer) {
    for (org.apache.parquet.format.ColumnOrder columnOrder : footer.getColumn_orders()) {
      if (columnOrder.isSetINT96_TIMESTAMP_ORDER()) {
        columnOrder.setTYPE_ORDER(new TypeDefinedOrder());
      }
    }
  }

  private static ColumnChunkMetaData getColumn(ParquetMetadata footer, String name) {
    BlockMetaData block = footer.getBlocks().get(0);
    return block.getColumns().stream()
        .filter(c -> c.getPath().toDotString().equals(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("no column " + name));
  }

  private static void assertStatsIgnored(ColumnChunkMetaData column) {
    assertTrue(column.getStatistics() == null || !column.getStatistics().hasNonNullValue());
  }

  private static void assertStatsUsable(ColumnChunkMetaData column) {
    assertTrue(column.getStatistics() != null && column.getStatistics().hasNonNullValue());
    assertArrayEquals(EXPECTED_MIN.getBytes(), column.getStatistics().getMinBytes());
    assertArrayEquals(EXPECTED_MAX.getBytes(), column.getStatistics().getMaxBytes());
  }

  @Test
  public void testWriterEmitsInt96StatsAndColumnOrder() throws IOException {
    // Values are written in non-chronological order; the writer must still produce the
    // chronological min/max, not first/last or byte-wise extremes.
    File file = writeFile();
    FileMetaData rawFooter = readRawFooter(file);
    // schema[0] is the message root; column_orders are indexed by leaf order: ts=0, id=1
    assertTrue(rawFooter.getColumn_orders().get(0).isSetINT96_TIMESTAMP_ORDER());
    assertTrue(rawFooter.getColumn_orders().get(1).isSetTYPE_ORDER());

    for (RowGroup rowGroup : rawFooter.getRow_groups()) {
      for (ColumnChunk chunk : rowGroup.getColumns()) {
        if (chunk.getMeta_data().getType() == Type.INT96) {
          Statistics stats = chunk.getMeta_data().getStatistics();
          assertTrue(stats != null && stats.isSetMin_value());
          assertArrayEquals(EXPECTED_MIN.getBytes(), stats.getMin_value());
          assertArrayEquals(EXPECTED_MAX.getBytes(), stats.getMax_value());
        }
      }
    }

    // Column index should be present for both columns.
    Configuration conf = new Configuration();
    Path path = new Path(file.getAbsolutePath());
    try (
      ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(path, conf), HadoopReadOptions.builder(conf, path).build()
      )
    ) {
      assertNotNull(reader.readColumnIndex(getColumn(reader.getFooter(), "id")));

      ColumnIndex columnIndex = reader.readColumnIndex(getColumn(reader.getFooter(), "ts"));
      assertNotNull(columnIndex);
      assertArrayEquals(EXPECTED_MIN.getBytes(), toArray(columnIndex.getMinValues().get(0)));
      assertArrayEquals(EXPECTED_MAX.getBytes(),
        toArray(columnIndex.getMaxValues().get(columnIndex.getMaxValues().size() - 1)));
    }
  }

  @Test
  public void testReaderReadsStatsWrittenWithInt96TimestampOrder() throws IOException {
    File file = writeFile();
    ParquetMetadata footer = readFooter(file, new Configuration());

    PrimitiveType ts = footer.getFileMetaData().getSchema().getType("ts").asPrimitiveType();
    assertEquals(ColumnOrder.int96TimestampOrder(), ts.columnOrder());
    assertEquals("BINARY_AS_INT96_TIMESTAMP_COMPARATOR", ts.comparator().toString());

    assertStatsUsable(getColumn(footer, "ts"));
  }

  @Test
  public void testReaderIgnoresInt96StatsWithTypeDefinedOrder() throws IOException {
    // Legacy layout: stats present, but column order is TYPE_ORDER so they are ignored.
    File file = writeFile();
    FileMetaData rawFooter = readRawFooter(file);
    downgradeInt96ToTypeOrder(rawFooter);
    File legacy = rewriteFooter(file, rawFooter, "type-defined-orders.parquet");

    // Validate the data is still intact after rewriting the footer.
    try (
      ParquetReader<Group> reader = ParquetReader.builder(
        new GroupReadSupport(), new Path(legacy.getAbsolutePath())
      ).build()
    ) {
      for (int i = 0; i < VALUES.size(); i++) {
        Group group = reader.read();
        assertEquals(VALUES.get(i), group.getInt96("ts", 0));
        assertEquals(i, group.getLong("id", 0));
      }
    }

    ParquetMetadata footer = readFooter(legacy, new Configuration());
    assertEquals(ColumnOrder.undefined(),
        footer.getFileMetaData().getSchema().getType("ts").asPrimitiveType().columnOrder());
    assertStatsIgnored(getColumn(footer, "ts"));
    // The non-INT96 sibling column is unaffected.
    assertTrue(getColumn(footer, "id").getStatistics().hasNonNullValue());
  }

  @Test
  public void testReaderIgnoresInt96StatsWhenColumnOrdersAbsent() throws IOException {
    // Legacy layout: INT96 stats present but the footer omits column_orders entirely (predating the
    // order). The reader must not infer INT96_TIMESTAMP_ORDER from the construction-time default
    // and must therefore ignore the stats.
    File file = writeFile();
    FileMetaData rawFooter = readRawFooter(file);
    rawFooter.unsetColumn_orders();
    File legacy = rewriteFooter(file, rawFooter, "no-column-orders.parquet");

    ParquetMetadata footer = readFooter(legacy, new Configuration());
    assertEquals(ColumnOrder.undefined(),
        footer.getFileMetaData().getSchema().getType("ts").asPrimitiveType().columnOrder());
    assertStatsIgnored(getColumn(footer, "ts"));
  }

  private static byte[] toArray(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return bytes;
  }
}
