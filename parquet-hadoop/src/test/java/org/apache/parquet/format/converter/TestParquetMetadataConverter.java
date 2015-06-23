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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.Type.INT32;
import static org.apache.parquet.format.Util.readPageHeader;
import static org.apache.parquet.format.Util.writePageHeader;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaData;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.getOffset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

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
    List<SchemaElement> parquetSchema = ParquetMetadataConverter.toParquetSchema(Paper.schema);
    MessageType schema = ParquetMetadataConverter.fromParquetSchema(parquetSchema);
    assertEquals(Paper.schema, schema);
  }

  @Test
  public void testSchemaConverterDecimal() {
    List<SchemaElement> schemaElements = ParquetMetadataConverter.toParquetSchema(
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
    for (org.apache.parquet.column.Encoding encoding : org.apache.parquet.column.Encoding.values()) {
      assertEquals(encoding, ParquetMetadataConverter.getEncoding(ParquetMetadataConverter.getEncoding(encoding)));
    }
    for (org.apache.parquet.format.Encoding encoding : org.apache.parquet.format.Encoding.values()) {
      assertEquals(encoding, ParquetMetadataConverter.getEncoding(ParquetMetadataConverter.getEncoding(encoding)));
    }
    for (Repetition repetition : Repetition.values()) {
      assertEquals(repetition, ParquetMetadataConverter.fromParquetRepetition(ParquetMetadataConverter.toParquetRepetition(repetition)));
    }
    for (FieldRepetitionType repetition : FieldRepetitionType.values()) {
      assertEquals(repetition, ParquetMetadataConverter.toParquetRepetition(ParquetMetadataConverter.fromParquetRepetition(repetition)));
    }
    for (PrimitiveTypeName primitiveTypeName : PrimitiveTypeName.values()) {
      assertEquals(primitiveTypeName, ParquetMetadataConverter.getPrimitive(ParquetMetadataConverter.getType(primitiveTypeName)));
    }
    for (Type type : Type.values()) {
      assertEquals(type, ParquetMetadataConverter.getType(ParquetMetadataConverter.getPrimitive(type)));
    }
    for (OriginalType original : OriginalType.values()) {
      assertEquals(original, ParquetMetadataConverter.getOriginalType(ParquetMetadataConverter.getConvertedType(original)));
    }
    for (ConvertedType converted : ConvertedType.values()) {
      assertEquals(converted, ParquetMetadataConverter.getConvertedType(ParquetMetadataConverter.getOriginalType(converted)));
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
    return filterFileMetaData(new FileMetaData(md), new ParquetMetadataConverter.RangeMetadataFilter(start, end));
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
  public void testEncodingsCache() {
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

    Set<org.apache.parquet.column.Encoding> res1 = ParquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res2 = ParquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res3 = ParquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy2);

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

}
