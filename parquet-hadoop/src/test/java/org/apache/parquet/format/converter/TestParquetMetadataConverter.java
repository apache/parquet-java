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
package org.apache.parquet.format.converter;

import static java.util.Collections.emptyList;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaDataByStart;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.Type.INT32;
import static org.apache.parquet.format.Util.readPageHeader;
import static org.apache.parquet.format.Util.writePageHeader;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaDataByMidpoint;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.getOffset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.junit.Assert;
import org.junit.Test;

import org.apache.parquet.example.Paper;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import com.google.common.collect.Lists;

public class TestParquetMetadataConverter {

  @Test
  public void testPageHeader() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PageType type = PageType.DATA_PAGE;
    int compSize = 10;
    int uncSize = 20;
    PageHeader pageHeader = new PageHeader(type, uncSize, compSize);
    writePageHeader(pageHeader, out);
    PageHeader readPageHeader = readPageHeader(new ByteArrayInputStream(out.toByteArray()));
    assertEquals(pageHeader, readPageHeader);
  }

  @Test
  public void testSchemaConverter() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(Paper.schema);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema);
    assertEquals(Paper.schema, schema);
  }

  @Test
  public void testSchemaConverterDecimal() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> schemaElements = parquetMetadataConverter.toParquetSchema(
        Types.buildMessage()
            .required(PrimitiveTypeName.BINARY)
                .as(OriginalType.DECIMAL).precision(9).scale(2)
                .named("aBinaryDecimal")
            .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(4)
                .as(OriginalType.DECIMAL).precision(9).scale(2)
                .named("aFixedDecimal")
            .named("Message")
    );
    List<SchemaElement> expected = Lists.newArrayList(
        new SchemaElement("Message").setNum_children(2),
        new SchemaElement("aBinaryDecimal")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setType(Type.BYTE_ARRAY)
            .setConverted_type(ConvertedType.DECIMAL)
            .setPrecision(9).setScale(2),
        new SchemaElement("aFixedDecimal")
            .setRepetition_type(FieldRepetitionType.OPTIONAL)
            .setType(Type.FIXED_LEN_BYTE_ARRAY)
            .setType_length(4)
            .setConverted_type(ConvertedType.DECIMAL)
            .setPrecision(9).setScale(2)
    );
    Assert.assertEquals(expected, schemaElements);
  }

  @Test
  public void testEnumEquivalence() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    for (org.apache.parquet.column.Encoding encoding : org.apache.parquet.column.Encoding.values()) {
      assertEquals(encoding, parquetMetadataConverter.getEncoding(parquetMetadataConverter.getEncoding(encoding)));
    }
    for (org.apache.parquet.format.Encoding encoding : org.apache.parquet.format.Encoding.values()) {
      assertEquals(encoding, parquetMetadataConverter.getEncoding(parquetMetadataConverter.getEncoding(encoding)));
    }
    for (Repetition repetition : Repetition.values()) {
      assertEquals(repetition, parquetMetadataConverter.fromParquetRepetition(parquetMetadataConverter.toParquetRepetition(repetition)));
    }
    for (FieldRepetitionType repetition : FieldRepetitionType.values()) {
      assertEquals(repetition, parquetMetadataConverter.toParquetRepetition(parquetMetadataConverter.fromParquetRepetition(repetition)));
    }
    for (PrimitiveTypeName primitiveTypeName : PrimitiveTypeName.values()) {
      assertEquals(primitiveTypeName, parquetMetadataConverter.getPrimitive(parquetMetadataConverter.getType(primitiveTypeName)));
    }
    for (Type type : Type.values()) {
      assertEquals(type, parquetMetadataConverter.getType(parquetMetadataConverter.getPrimitive(type)));
    }
    for (OriginalType original : OriginalType.values()) {
      assertEquals(original, parquetMetadataConverter.getOriginalType(parquetMetadataConverter.getConvertedType(original)));
    }
    for (ConvertedType converted : ConvertedType.values()) {
      assertEquals(converted, parquetMetadataConverter.getConvertedType(parquetMetadataConverter.getOriginalType(converted)));
    }
  }

  private FileMetaData metadata(long... sizes) {
    List<SchemaElement> schema = emptyList();
    List<RowGroup> rowGroups = new ArrayList<RowGroup>();
    long offset = 0;
    for (long size : sizes) {
      ColumnChunk columnChunk = new ColumnChunk(offset);
      columnChunk.setMeta_data(new ColumnMetaData(
          INT32,
          Collections.<org.apache.parquet.format.Encoding>emptyList(),
          Collections.<String>emptyList(),
          UNCOMPRESSED, 10l, size * 2, size, offset));
      rowGroups.add(new RowGroup(Arrays.asList(columnChunk), size, 1));
      offset += size;
    }
    return new FileMetaData(1, schema, sizes.length, rowGroups);
  }

  private FileMetaData filter(FileMetaData md, long start, long end) {
    return filterFileMetaDataByMidpoint(new FileMetaData(md),
        new ParquetMetadataConverter.RangeMetadataFilter(start, end));
  }

  private FileMetaData find(FileMetaData md, Long... blockStart) {
    return filterFileMetaDataByStart(new FileMetaData(md),
        new ParquetMetadataConverter.OffsetMetadataFilter(
            Sets.newHashSet((Long[]) blockStart)));
  }

  private FileMetaData find(FileMetaData md, long blockStart) {
    return filterFileMetaDataByStart(new FileMetaData(md),
        new ParquetMetadataConverter.OffsetMetadataFilter(
            Sets.newHashSet(blockStart)));
  }

  private void verifyMD(FileMetaData md, long... offsets) {
    assertEquals(offsets.length, md.row_groups.size());
    for (int i = 0; i < offsets.length; i++) {
      long offset = offsets[i];
      RowGroup rowGroup = md.getRow_groups().get(i);
      assertEquals(offset, getOffset(rowGroup));
    }
  }

  /**
   * verifies that splits will end up being a partition of the rowgroup
   * they are all found only once
   * @param md
   * @param splitWidth
   */
  private void verifyAllFilters(FileMetaData md, long splitWidth) {
    Set<Long> offsetsFound = new TreeSet<Long>();
    for (long start = 0; start < fileSize(md); start += splitWidth) {
      FileMetaData filtered = filter(md, start, start + splitWidth);
      for (RowGroup rg : filtered.getRow_groups()) {
        long o = getOffset(rg);
        if (offsetsFound.contains(o)) {
          fail("found the offset twice: " + o);
        } else {
          offsetsFound.add(o);
        }
      }
    }
    if (offsetsFound.size() != md.row_groups.size()) {
      fail("missing row groups, "
          + "found: " + offsetsFound
          + "\nexpected " + md.getRow_groups());
    }
  }

  private long fileSize(FileMetaData md) {
    long size = 0;
    for (RowGroup rg : md.getRow_groups()) {
      size += rg.total_byte_size;
    }
    return size;
  }

  @Test
  public void testFilterMetaData() {
    verifyMD(filter(metadata(50, 50, 50), 0, 50), 0);
    verifyMD(filter(metadata(50, 50, 50), 50, 100), 50);
    verifyMD(filter(metadata(50, 50, 50), 100, 150), 100);
    // picks up first RG
    verifyMD(filter(metadata(50, 50, 50), 25, 75), 0);
    // picks up no RG
    verifyMD(filter(metadata(50, 50, 50), 26, 75));
    // picks up second RG
    verifyMD(filter(metadata(50, 50, 50), 26, 76), 50);

    verifyAllFilters(metadata(50, 50, 50), 10);
    verifyAllFilters(metadata(50, 50, 50), 51);
    verifyAllFilters(metadata(50, 50, 50), 25); // corner cases are in the middle
    verifyAllFilters(metadata(50, 50, 50), 24);
    verifyAllFilters(metadata(50, 50, 50), 26);
    verifyAllFilters(metadata(50, 50, 50), 110);
    verifyAllFilters(metadata(10, 50, 500), 110);
    verifyAllFilters(metadata(10, 50, 500), 10);
    verifyAllFilters(metadata(10, 50, 500), 600);
    verifyAllFilters(metadata(11, 9, 10), 10);
    verifyAllFilters(metadata(11, 9, 10), 9);
    verifyAllFilters(metadata(11, 9, 10), 8);
  }

  @Test
  public void testFindRowGroups() {
    verifyMD(find(metadata(50, 50, 50), 0), 0);
    verifyMD(find(metadata(50, 50, 50), 50), 50);
    verifyMD(find(metadata(50, 50, 50), 100), 100);
    verifyMD(find(metadata(50, 50, 50), 0L, 50L), 0, 50);
    verifyMD(find(metadata(50, 50, 50), 0L, 50L, 100L), 0, 50, 100);
    verifyMD(find(metadata(50, 50, 50), 50L, 100L), 50, 100);
    // doesn't find an offset that isn't the start of a row group.
    verifyMD(find(metadata(50, 50, 50), 10));
  }

  @Test
  public void randomTestFilterMetaData() {
    // randomized property based testing
    // if it fails add the case above
    Random random = new Random(System.currentTimeMillis());
    for (int j = 0; j < 100; j++) {
      long[] rgs = new long[random.nextInt(50)];
      for (int i = 0; i < rgs.length; i++) {
        rgs[i] = random.nextInt(10000) + 1; // No empty row groups
      }
      int splitSize = random.nextInt(10000);
      try {
        verifyAllFilters(metadata(rgs), splitSize);
      } catch (AssertionError e) {
	  throw (AssertionError) new AssertionError("fail verifyAllFilters(metadata(" + Arrays.toString(rgs) + "), " + splitSize + ")").initCause(e);
      }
    }
  }

  @Test
  public void testNullFieldMetadataDebugLogging() {
    MessageType schema = parseMessageType("message test { optional binary some_null_field; }");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData = new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<String, String>(), null);
    List<BlockMetaData> blockMetaDataList = new ArrayList<BlockMetaData>();
    BlockMetaData blockMetaData = new BlockMetaData();
    blockMetaData.addColumn(createColumnChunkMetaData());
    blockMetaDataList.add(blockMetaData);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, blockMetaDataList);
    ParquetMetadata.toJSON(metadata);
  }

  @Test
  public void testMetadataToJson() {
    ParquetMetadata metadata = new ParquetMetadata(null, null);
    assertEquals("{\"fileMetaData\":null,\"blocks\":null}", ParquetMetadata.toJSON(metadata));
    assertEquals("{\n" +
            "  \"fileMetaData\" : null,\n" +
            "  \"blocks\" : null\n" +
            "}", ParquetMetadata.toPrettyJSON(metadata));
  }

  private ColumnChunkMetaData createColumnChunkMetaData() {
    Set<org.apache.parquet.column.Encoding> e = new HashSet<org.apache.parquet.column.Encoding>();
    PrimitiveTypeName t = PrimitiveTypeName.BINARY;
    ColumnPath p = ColumnPath.get("foo");
    CompressionCodecName c = CompressionCodecName.GZIP;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, e, s,
            0, 0, 0, 0, 0);
    return md;
  }
  
  @Test
  public void testEncodingsCache() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    List<org.apache.parquet.format.Encoding> formatEncodingsCopy1 =
        Arrays.asList(org.apache.parquet.format.Encoding.BIT_PACKED,
                      org.apache.parquet.format.Encoding.RLE_DICTIONARY,
                      org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    List<org.apache.parquet.format.Encoding> formatEncodingsCopy2 =
        Arrays.asList(org.apache.parquet.format.Encoding.BIT_PACKED,
            org.apache.parquet.format.Encoding.RLE_DICTIONARY,
            org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    Set<org.apache.parquet.column.Encoding> expected = new HashSet<org.apache.parquet.column.Encoding>();
    expected.add(org.apache.parquet.column.Encoding.BIT_PACKED);
    expected.add(org.apache.parquet.column.Encoding.RLE_DICTIONARY);
    expected.add(org.apache.parquet.column.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    Set<org.apache.parquet.column.Encoding> res1 = parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res2 = parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res3 = parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy2);

    // make sure they are all semantically equal
    assertEquals(expected, res1);
    assertEquals(expected, res2);
    assertEquals(expected, res3);

    // make sure res1, res2, and res3 are actually the same cached object
    assertSame(res1, res2);
    assertSame(res1, res3);

    // make sure they are all unmodifiable (UnmodifiableSet is not public, so we have to compare on class name)
    assertEquals("java.util.Collections$UnmodifiableSet", res1.getClass().getName());
    assertEquals("java.util.Collections$UnmodifiableSet", res2.getClass().getName());
    assertEquals("java.util.Collections$UnmodifiableSet", res3.getClass().getName());
  }

  @Test
  public void testBinaryStats() {
    // make fake stats and verify the size check
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls(3004);
    byte[] min = new byte[904];
    byte[] max = new byte[2388];
    stats.updateStats(Binary.fromConstantByteArray(min));
    stats.updateStats(Binary.fromConstantByteArray(max));
    long totalLen = min.length + max.length;
    Assert.assertFalse("Should not be smaller than min + max size",
        stats.isSmallerThan(totalLen));
    Assert.assertTrue("Should be smaller than min + max size + 1",
        stats.isSmallerThan(totalLen + 1));

    org.apache.parquet.format.Statistics formatStats =
        ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertArrayEquals("Min should match", min, formatStats.getMin());
    Assert.assertArrayEquals("Max should match", max, formatStats.getMax());
    Assert.assertEquals("Num nulls should match",
        3004, formatStats.getNull_count());

    // convert to empty stats because the values are too large
    stats.setMinMaxFromBytes(max, max);

    formatStats = ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertFalse("Min should not be set", formatStats.isSetMin());
    Assert.assertFalse("Max should not be set", formatStats.isSetMax());
    Assert.assertFalse("Num nulls should not be set",
        formatStats.isSetNull_count());

    Statistics roundTripStats = ParquetMetadataConverter.fromParquetStatistics(
        Version.FULL_VERSION, formatStats, PrimitiveTypeName.BINARY);

    Assert.assertTrue(roundTripStats.isEmpty());
  }

  @Test
  public void testIntegerStats() {
    // make fake stats and verify the size check
    IntStatistics stats = new IntStatistics();
    stats.incrementNumNulls(3004);
    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats =
        ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertEquals("Min should match",
        min, BytesUtils.bytesToInt(formatStats.getMin()));
    Assert.assertEquals("Max should match",
        max, BytesUtils.bytesToInt(formatStats.getMax()));
    Assert.assertEquals("Num nulls should match",
        3004, formatStats.getNull_count());
  }

  @Test
  public void testLongStats() {
    // make fake stats and verify the size check
    LongStatistics stats = new LongStatistics();
    stats.incrementNumNulls(3004);
    long min = Long.MIN_VALUE;
    long max = Long.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats =
        ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertEquals("Min should match",
        min, BytesUtils.bytesToLong(formatStats.getMin()));
    Assert.assertEquals("Max should match",
        max, BytesUtils.bytesToLong(formatStats.getMax()));
    Assert.assertEquals("Num nulls should match",
        3004, formatStats.getNull_count());
  }

  @Test
  public void testFloatStats() {
    // make fake stats and verify the size check
    FloatStatistics stats = new FloatStatistics();
    stats.incrementNumNulls(3004);
    float min = Float.MIN_VALUE;
    float max = Float.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats =
        ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertEquals("Min should match",
        min, Float.intBitsToFloat(BytesUtils.bytesToInt(formatStats.getMin())),
        0.000001);
    Assert.assertEquals("Max should match",
        max, Float.intBitsToFloat(BytesUtils.bytesToInt(formatStats.getMax())),
        0.000001);
    Assert.assertEquals("Num nulls should match",
        3004, formatStats.getNull_count());
  }

  @Test
  public void testDoubleStats() {
    // make fake stats and verify the size check
    DoubleStatistics stats = new DoubleStatistics();
    stats.incrementNumNulls(3004);
    double min = Double.MIN_VALUE;
    double max = Double.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats =
        ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertEquals("Min should match",
        min, Double.longBitsToDouble(BytesUtils.bytesToLong(formatStats.getMin())),
        0.000001);
    Assert.assertEquals("Max should match",
        max, Double.longBitsToDouble(BytesUtils.bytesToLong(formatStats.getMax())),
        0.000001);
    Assert.assertEquals("Num nulls should match",
        3004, formatStats.getNull_count());
  }

  @Test
  public void testBooleanStats() {
    // make fake stats and verify the size check
    BooleanStatistics stats = new BooleanStatistics();
    stats.incrementNumNulls(3004);
    boolean min = Boolean.FALSE;
    boolean max = Boolean.TRUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats =
        ParquetMetadataConverter.toParquetStatistics(stats);

    Assert.assertEquals("Min should match",
        min, BytesUtils.bytesToBool(formatStats.getMin()));
    Assert.assertEquals("Max should match",
        max, BytesUtils.bytesToBool(formatStats.getMax()));
    Assert.assertEquals("Num nulls should match",
        3004, formatStats.getNull_count());
  }
}
