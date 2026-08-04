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

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static org.apache.parquet.filter2.dictionarylevel.DictionaryFilter.canDrop;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.contains;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notIn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.FixedBinaryTestUtils;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.FloatColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class DictionaryFilterTest {

  private static final int nElements = 1000;
  private static final Configuration conf = new Configuration();
  private static final Path FILE_V1 = new Path("target/test/TestDictionaryFilter/testParquetFileV1.parquet");
  private static final Path FILE_V2 = new Path("target/test/TestDictionaryFilter/testParquetFileV2.parquet");
  private static final double DOUBLE_NAN_A = Double.longBitsToDouble(0x7ff8000000000001L);
  private static final double DOUBLE_NAN_B = Double.longBitsToDouble(0x7ff8000000000002L);
  private static final double NEGATIVE_DOUBLE_NAN = Double.longBitsToDouble(0xfff8000000000001L);
  private static final float FLOAT_NAN_A = Float.intBitsToFloat(0x7fc00001);
  private static final float FLOAT_NAN_B = Float.intBitsToFloat(0x7fc00002);
  private static final float NEGATIVE_FLOAT_NAN = Float.intBitsToFloat(0xffc00001);
  private static final Binary FLOAT16_NAN_A = Binary.fromConstantByteArray(new byte[] {0x01, 0x7e});
  private static final Binary FLOAT16_NAN_B = Binary.fromConstantByteArray(new byte[] {0x02, 0x7e});
  private static final MessageType schema = parseMessageType("message test { "
      + "required binary binary_field; "
      + "required binary single_value_field; "
      + "optional binary optional_single_value_field; "
      + "optional int32 optional_single_value_int32_field;"
      + "required fixed_len_byte_array(17) fixed_field (DECIMAL(40,4)); "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required double double_field; "
      + "required float float_field; "
      + "required int32 plain_int32_field; "
      + "required binary fallback_binary_field; "
      + "required int96 int96_field; "
      + "repeated binary repeated_binary_field;"
      + "} ");
  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
  private static final int[] intValues = new int[] {
    -100, 302, 3333333, 7654321, 1234567, -2000, -77775, 0, 75, 22223,
    77, 22221, -444443, 205, 12, 44444, 889, 66665, -777889, -7,
    52, 33, -257, 1111, 775, 26
  };
  private static final long[] longValues = new long[] {
    -100L, 302L, 3333333L, 7654321L, 1234567L, -2000L, -77775L, 0L, 75L, 22223L, 77L, 22221L, -444443L, 205L, 12L,
    44444L, 889L, 66665L, -777889L, -7L, 52L, 33L, -257L, 1111L, 775L, 26L
  };
  private static final Binary[] DECIMAL_VALUES = new Binary[] {
    toBinary("-9999999999999999999999999999999999999999", 17),
    toBinary("-9999999999999999999999999999999999999998", 17),
    toBinary(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), 17),
    toBinary(BigInteger.valueOf(Long.MIN_VALUE), 17),
    toBinary(BigInteger.valueOf(Long.MIN_VALUE).add(BigInteger.ONE), 17),
    toBinary("-1", 17),
    toBinary("0", 17),
    toBinary(BigInteger.valueOf(Long.MAX_VALUE).subtract(BigInteger.ONE), 17),
    toBinary(BigInteger.valueOf(Long.MAX_VALUE), 17),
    toBinary(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), 17),
    toBinary("999999999999999999999999999999999999999", 17),
    toBinary("9999999999999999999999999999999999999998", 17),
    toBinary("9999999999999999999999999999999999999999", 17)
  };
  private static final Binary[] INT96_VALUES = new Binary[] {
    toBinary("-9999999999999999999999999999", 12),
    toBinary("-9999999999999999999999999998", 12),
    toBinary("-1234567890", 12),
    toBinary("-1", 12),
    toBinary("-0", 12),
    toBinary("1", 12),
    toBinary("1234567890", 12),
    toBinary("-9999999999999999999999999998", 12),
    toBinary("9999999999999999999999999999", 12)
  };

  private static Binary toBinary(String decimalWithoutScale, int byteCount) {
    return toBinary(new BigInteger(decimalWithoutScale), byteCount);
  }

  private static Binary toBinary(BigInteger decimalWithoutScale, int byteCount) {
    return FixedBinaryTestUtils.getFixedBinary(byteCount, decimalWithoutScale);
  }

  private static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      int index = i % ALPHABET.length();

      Group group = f.newGroup()
          .append("binary_field", ALPHABET.substring(index, index + 1))
          .append("single_value_field", "sharp")
          .append("fixed_field", DECIMAL_VALUES[i % DECIMAL_VALUES.length])
          .append("int32_field", intValues[i % intValues.length])
          .append("int64_field", longValues[i % longValues.length])
          .append("double_field", toDouble(intValues[i % intValues.length]))
          .append("float_field", toFloat(intValues[i % intValues.length]))
          .append("plain_int32_field", i)
          .append(
              "fallback_binary_field",
              i < (nElements / 2)
                  ? ALPHABET.substring(index, index + 1)
                  : UUID.randomUUID().toString())
          .append("int96_field", INT96_VALUES[i % INT96_VALUES.length])
          .append("repeated_binary_field", ALPHABET.substring(index, index + 1));

      if (index + 1 < 26) {
        group = group.append("repeated_binary_field", ALPHABET.substring(index + 1, index + 2));
      }

      if (index + 2 < 26) {
        group = group.append("repeated_binary_field", ALPHABET.substring(index + 2, index + 3));
      }

      // 10% of the time, leave the field null
      if (index % 10 > 0) {
        group.append("optional_single_value_field", "sharp");
        group.append("optional_single_value_int32_field", 42);
      }

      writer.write(group);
    }
    writer.close();
  }

  @BeforeAll
  public static void prepareFile() throws IOException {
    cleanup();
    prepareFile(PARQUET_1_0, FILE_V1);
    prepareFile(PARQUET_2_0, FILE_V2);
  }

  private static void prepareFile(WriterVersion version, Path file) throws IOException {
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withWriterVersion(version)
        .withCompressionCodec(GZIP)
        .withRowGroupSize(1024 * 1024)
        .withPageSize(1024)
        .enableDictionaryEncoding()
        .withDictionaryPageSize(2 * 1024)
        .withConf(conf)
        .build();
    writeData(f, writer);
  }

  @AfterAll
  public static void cleanup() throws IOException {
    deleteFile(FILE_V1);
    deleteFile(FILE_V2);
  }

  private static void deleteFile(Path file) throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    if (fs.exists(file)) {
      fs.delete(file, true);
    }
  }

  List<ColumnChunkMetaData> ccmd;
  ParquetFileReader reader;
  DictionaryPageReadStore dictionaries;

  private static Path fileForWriterVersion(WriterVersion version) {
    switch (version) {
      case PARQUET_1_0:
        return FILE_V1;
      case PARQUET_2_0:
        return FILE_V2;
      default:
        throw new IllegalArgumentException("Unexpected writer version: " + version);
    }
  }

  private void setUp(WriterVersion version) throws Exception {
    reader = ParquetFileReader.open(conf, fileForWriterVersion(version));
    ParquetMetadata meta = reader.getFooter();
    ccmd = meta.getBlocks().get(0).getColumns();
    dictionaries = reader.getDictionaryReader(meta.getBlocks().get(0));
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testDictionaryEncodedColumns(WriterVersion version) throws Exception {
    setUp(version);
    switch (version) {
      case PARQUET_1_0:
        testDictionaryEncodedColumnsV1();
        break;
      case PARQUET_2_0:
        testDictionaryEncodedColumnsV2();
        break;
    }
  }

  @SuppressWarnings("deprecation")
  private void testDictionaryEncodedColumnsV1() {
    Set<String> dictionaryEncodedColumns = new HashSet<String>(List.of(
        "binary_field",
        "single_value_field",
        "optional_single_value_field",
        "optional_single_value_int32_field",
        "int32_field",
        "int64_field",
        "double_field",
        "float_field",
        "int96_field",
        "repeated_binary_field"));
    for (ColumnChunkMetaData column : ccmd) {
      String name = column.getPath().toDotString();
      if (dictionaryEncodedColumns.contains(name)) {
        assertThat(column.getEncodings())
            .as("Column should be dictionary encoded: " + name)
            .contains(Encoding.PLAIN_DICTIONARY);
        assertThat(column.getEncodings())
            .as("Column should not have plain data pages" + name)
            .doesNotContain(Encoding.PLAIN);
      } else {
        assertThat(column.getEncodings())
            .as("Column should have plain encoding: " + name)
            .contains(Encoding.PLAIN);
        if (name.startsWith("fallback")) {
          assertThat(column.getEncodings())
              .as("Column should have some dictionary encoding: " + name)
              .contains(Encoding.PLAIN_DICTIONARY);
        } else {
          assertThat(column.getEncodings())
              .as("Column should have no dictionary encoding: " + name)
              .doesNotContain(Encoding.PLAIN_DICTIONARY);
        }
      }
    }
  }

  private void testDictionaryEncodedColumnsV2() {
    Set<String> dictionaryEncodedColumns = new HashSet<String>(List.of(
        "binary_field",
        "single_value_field",
        "optional_single_value_field",
        "optional_single_value_int32_field",
        "fixed_field",
        "int32_field",
        "int64_field",
        "double_field",
        "float_field",
        "int96_field",
        "repeated_binary_field"));
    for (ColumnChunkMetaData column : ccmd) {
      EncodingStats encStats = column.getEncodingStats();
      String name = column.getPath().toDotString();
      if (dictionaryEncodedColumns.contains(name)) {
        assertThat(encStats.hasDictionaryPages())
            .as("Column should have dictionary pages: " + name)
            .isTrue();
        assertThat(encStats.hasDictionaryEncodedPages())
            .as("Column should have dictionary encoded pages: " + name)
            .isTrue();
        assertThat(encStats.hasNonDictionaryEncodedPages())
            .as("Column should not have non-dictionary encoded pages: " + name)
            .isFalse();
      } else {
        assertThat(encStats.hasNonDictionaryEncodedPages())
            .as("Column should have non-dictionary encoded pages: " + name)
            .isTrue();
        if (name.startsWith("fallback")) {
          assertThat(encStats.hasDictionaryPages())
              .as("Column should have dictionary pages: " + name)
              .isTrue();
          assertThat(encStats.hasDictionaryEncodedPages())
              .as("Column should have dictionary encoded pages: " + name)
              .isTrue();
        } else {
          assertThat(encStats.hasDictionaryPages())
              .as("Column should not have dictionary pages: " + name)
              .isFalse();
          assertThat(encStats.hasDictionaryEncodedPages())
              .as("Column should not have dictionary encoded pages: " + name)
              .isFalse();
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testEqBinary(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("binary_field");
    FilterPredicate pred = eq(b, Binary.fromString("c"));

    assertThat(canDrop(pred, ccmd, dictionaries))
        .as("Should not drop block for lower case letters")
        .isFalse();

    assertThat(canDrop(eq(b, Binary.fromString("A")), ccmd, dictionaries))
        .as("Should drop block for upper case letters")
        .isTrue();

    assertThat(canDrop(eq(b, null), ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testEqFixed(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("fixed_field");

    // Only V2 supports dictionary encoding for FIXED_LEN_BYTE_ARRAY values
    if (version == PARQUET_2_0) {
      assertThat(canDrop(eq(b, toBinary("-2", 17)), ccmd, dictionaries))
          .as("Should drop block for -2")
          .isTrue();
    }

    assertThat(canDrop(eq(b, toBinary("-1", 17)), ccmd, dictionaries))
        .as("Should not drop block for -1")
        .isFalse();

    assertThat(canDrop(eq(b, null), ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testEqInt96(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("int96_field");

    // INT96 ordering is undefined => no filtering shall be done
    assertThat(canDrop(eq(b, toBinary("-2", 12)), ccmd, dictionaries))
        .as("Should not drop block for -2")
        .isFalse();

    assertThat(canDrop(eq(b, toBinary("-1", 12)), ccmd, dictionaries))
        .as("Should not drop block for -1")
        .isFalse();

    assertThat(canDrop(eq(b, null), ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testNotEqBinary(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn sharp = binaryColumn("single_value_field");
    BinaryColumn sharpAndNull = binaryColumn("optional_single_value_field");
    BinaryColumn b = binaryColumn("binary_field");

    assertThat(canDrop(notEq(sharp, Binary.fromString("sharp")), ccmd, dictionaries))
        .as("Should drop block with only the excluded value")
        .isTrue();

    assertThat(canDrop(notEq(sharp, Binary.fromString("applause")), ccmd, dictionaries))
        .as("Should not drop block with any other value")
        .isFalse();

    assertThat(canDrop(notEq(sharpAndNull, Binary.fromString("sharp")), ccmd, dictionaries))
        .as("Should not drop block with only the excluded value and null")
        .isFalse();

    assertThat(canDrop(notEq(sharpAndNull, Binary.fromString("applause")), ccmd, dictionaries))
        .as("Should not drop block with any other value")
        .isFalse();

    assertThat(canDrop(notEq(b, Binary.fromString("x")), ccmd, dictionaries))
        .as("Should not drop block with a known value")
        .isFalse();

    assertThat(canDrop(notEq(b, Binary.fromString("B")), ccmd, dictionaries))
        .as("Should not drop block with a known value")
        .isFalse();

    assertThat(canDrop(notEq(b, null), ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testLtInt(WriterVersion version) throws Exception {
    setUp(version);
    IntColumn i32 = intColumn("int32_field");
    int lowest = Integer.MAX_VALUE;
    for (int value : intValues) {
      lowest = Math.min(lowest, value);
    }

    assertThat(canDrop(lt(i32, lowest), ccmd, dictionaries))
        .as("Should drop: < lowest value")
        .isTrue();
    assertThat(canDrop(lt(i32, lowest + 1), ccmd, dictionaries))
        .as("Should not drop: < (lowest value + 1)")
        .isFalse();

    assertThat(canDrop(lt(i32, Integer.MAX_VALUE), ccmd, dictionaries))
        .as("Should not drop: contains matching values")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testLtFixed(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn fixed = binaryColumn("fixed_field");

    // Only V2 supports dictionary encoding for FIXED_LEN_BYTE_ARRAY values
    if (version == PARQUET_2_0) {
      assertThat(canDrop(lt(fixed, DECIMAL_VALUES[0]), ccmd, dictionaries))
          .as("Should drop: < lowest value")
          .isTrue();
    }

    assertThat(canDrop(lt(fixed, DECIMAL_VALUES[1]), ccmd, dictionaries))
        .as("Should not drop: < 2nd lowest value")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testLtEqLong(WriterVersion version) throws Exception {
    setUp(version);
    LongColumn i64 = longColumn("int64_field");
    long lowest = Long.MAX_VALUE;
    for (long value : longValues) {
      lowest = Math.min(lowest, value);
    }

    assertThat(canDrop(ltEq(i64, lowest - 1), ccmd, dictionaries))
        .as("Should drop: <= lowest - 1")
        .isTrue();
    assertThat(canDrop(ltEq(i64, lowest), ccmd, dictionaries))
        .as("Should not drop: <= lowest")
        .isFalse();

    assertThat(canDrop(ltEq(i64, Long.MAX_VALUE), ccmd, dictionaries))
        .as("Should not drop: contains matching values")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testGtFloat(WriterVersion version) throws Exception {
    setUp(version);
    FloatColumn f = floatColumn("float_field");
    float highest = Float.MIN_VALUE;
    for (int value : intValues) {
      highest = Math.max(highest, toFloat(value));
    }

    assertThat(canDrop(gt(f, highest), ccmd, dictionaries))
        .as("Should drop: > highest value")
        .isTrue();
    assertThat(canDrop(gt(f, highest - 1.0f), ccmd, dictionaries))
        .as("Should not drop: > (highest value - 1.0)")
        .isFalse();

    assertThat(canDrop(gt(f, Float.MIN_VALUE), ccmd, dictionaries))
        .as("Should not drop: contains matching values")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testGtEqDouble(WriterVersion version) throws Exception {
    setUp(version);
    DoubleColumn d = doubleColumn("double_field");
    double highest = Double.MIN_VALUE;
    for (int value : intValues) {
      highest = Math.max(highest, toDouble(value));
    }

    assertThat(canDrop(gtEq(d, highest + 0.00000001), ccmd, dictionaries))
        .as("Should drop: >= highest + 0.00000001")
        .isTrue();
    assertThat(canDrop(gtEq(d, highest), ccmd, dictionaries))
        .as("Should not drop: >= highest")
        .isFalse();

    assertThat(canDrop(gtEq(d, Double.MIN_VALUE), ccmd, dictionaries))
        .as("Should not drop: contains matching values")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testNaNDictionaryFilterIsConservative(WriterVersion version) throws Exception {
    setUp(version);
    List<ColumnChunkMetaData> nanColumns = nanColumns();
    DictionaryPageReadStore nanDictionaries = nanDictionaries();
    DoubleColumn doubleColumn = doubleColumn("double_nan_field");
    FloatColumn floatColumn = floatColumn("float_nan_field");
    BinaryColumn float16Column = binaryColumn("float16_nan_field");

    assertThat(canDrop(eq(doubleColumn, DOUBLE_NAN_A), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(eq(doubleColumn, DOUBLE_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notEq(doubleColumn, DOUBLE_NAN_A), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notEq(doubleColumn, DOUBLE_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(lt(doubleColumn, DOUBLE_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(gt(doubleColumn, DOUBLE_NAN_B), nanColumns, nanDictionaries))
        .isFalse();

    assertThat(canDrop(eq(floatColumn, FLOAT_NAN_A), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(eq(floatColumn, FLOAT_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notEq(floatColumn, FLOAT_NAN_A), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notEq(floatColumn, FLOAT_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(lt(floatColumn, FLOAT_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(gt(floatColumn, FLOAT_NAN_B), nanColumns, nanDictionaries))
        .isFalse();

    Set<Double> doubleSet = new HashSet<>();
    doubleSet.add(DOUBLE_NAN_B);
    assertThat(canDrop(in(doubleColumn, doubleSet), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notIn(doubleColumn, doubleSet), nanColumns, nanDictionaries))
        .isFalse();

    Set<Float> floatSet = new HashSet<>();
    floatSet.add(FLOAT_NAN_B);
    assertThat(canDrop(in(floatColumn, floatSet), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notIn(floatColumn, floatSet), nanColumns, nanDictionaries))
        .isFalse();

    Set<Binary> float16Set = new HashSet<>();
    float16Set.add(FLOAT16_NAN_B);
    assertThat(canDrop(in(float16Column, float16Set), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notIn(float16Column, float16Set), nanColumns, nanDictionaries))
        .isFalse();

    assertThat(canDrop(eq(float16Column, FLOAT16_NAN_A), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(eq(float16Column, FLOAT16_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notEq(float16Column, FLOAT16_NAN_A), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(notEq(float16Column, FLOAT16_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(lt(float16Column, FLOAT16_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
    assertThat(canDrop(gt(float16Column, FLOAT16_NAN_B), nanColumns, nanDictionaries))
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testNaNDictionaryValuesMakeRangeFiltersConservative(WriterVersion version) throws Exception {
    setUp(version);
    DictionaryPageReadStore dictionariesWithNaNs = dictionariesWithNaNs();
    assertNaNDictionaryRangeFiltersAreConservative(nanColumns(), dictionariesWithNaNs);
    assertNaNDictionaryRangeFiltersAreConservative(ieeeNaNColumns(), dictionariesWithNaNs);
  }

  private static void assertNaNDictionaryRangeFiltersAreConservative(
      List<ColumnChunkMetaData> nanColumns, DictionaryPageReadStore dictionariesWithNaNs) throws IOException {
    DoubleColumn doubleColumn = doubleColumn("double_nan_field");
    FloatColumn floatColumn = floatColumn("float_nan_field");
    BinaryColumn float16Column = binaryColumn("float16_nan_field");

    assertThat(canDrop(gt(doubleColumn, 0.0D), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(gtEq(doubleColumn, 0.0D), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(lt(doubleColumn, 0.0D), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(ltEq(doubleColumn, 0.0D), nanColumns, dictionariesWithNaNs))
        .isFalse();

    assertThat(canDrop(gt(floatColumn, 0.0F), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(gtEq(floatColumn, 0.0F), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(lt(floatColumn, 0.0F), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(ltEq(floatColumn, 0.0F), nanColumns, dictionariesWithNaNs))
        .isFalse();

    Binary zero = Binary.fromConstantByteArray(new byte[] {0x00, 0x00});
    assertThat(canDrop(gt(float16Column, zero), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(gtEq(float16Column, zero), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(lt(float16Column, zero), nanColumns, dictionariesWithNaNs))
        .isFalse();
    assertThat(canDrop(ltEq(float16Column, zero), nanColumns, dictionariesWithNaNs))
        .isFalse();
  }

  private static List<ColumnChunkMetaData> nanColumns() {
    return List.of(
        nanColumn(
            "double_nan_field",
            Types.required(PrimitiveTypeName.DOUBLE).named("double_nan_field")),
        nanColumn(
            "float_nan_field",
            Types.required(PrimitiveTypeName.FLOAT).named("float_nan_field")),
        nanColumn(
            "float16_nan_field",
            Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(2)
                .as(LogicalTypeAnnotation.float16Type())
                .named("float16_nan_field")));
  }

  private static List<ColumnChunkMetaData> ieeeNaNColumns() {
    return List.of(
        nanColumn(
            "double_nan_field",
            Types.required(PrimitiveTypeName.DOUBLE)
                .columnOrder(ColumnOrder.ieee754TotalOrder())
                .named("double_nan_field")),
        nanColumn(
            "float_nan_field",
            Types.required(PrimitiveTypeName.FLOAT)
                .columnOrder(ColumnOrder.ieee754TotalOrder())
                .named("float_nan_field")),
        nanColumn(
            "float16_nan_field",
            Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(2)
                .as(LogicalTypeAnnotation.float16Type())
                .columnOrder(ColumnOrder.ieee754TotalOrder())
                .named("float16_nan_field")));
  }

  private static ColumnChunkMetaData nanColumn(String name, PrimitiveType type) {
    EncodingStats encodingStats = new EncodingStats.Builder()
        .addDictEncoding(Encoding.PLAIN)
        .addDataEncoding(Encoding.RLE_DICTIONARY)
        .build();
    org.apache.parquet.column.statistics.Statistics<?> stats =
        org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type)
            .build();
    stats.setNumNulls(0);
    return ColumnChunkMetaData.get(
        ColumnPath.get(name),
        type,
        CompressionCodecName.UNCOMPRESSED,
        encodingStats,
        new HashSet<>(List.of(Encoding.PLAIN, Encoding.RLE_DICTIONARY)),
        stats,
        0L,
        0L,
        1L,
        0L,
        0L);
  }

  private static DictionaryPageReadStore nanDictionaries() {
    return new DictionaryPageReadStore() {
      @Override
      public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
        throw new AssertionError("NaN literals should not read dictionary pages");
      }
    };
  }

  private static DictionaryPageReadStore dictionariesWithNaNs() {
    return new DictionaryPageReadStore() {
      @Override
      public DictionaryPage readDictionaryPage(ColumnDescriptor descriptor) {
        PrimitiveTypeName type = descriptor.getPrimitiveType().getPrimitiveTypeName();
        switch (type) {
          case DOUBLE:
            return new DictionaryPage(
                BytesInput.from(littleEndianLongs(
                    Double.doubleToRawLongBits(NEGATIVE_DOUBLE_NAN),
                    Double.doubleToRawLongBits(DOUBLE_NAN_A))),
                2,
                Encoding.PLAIN);
          case FLOAT:
            return new DictionaryPage(
                BytesInput.from(littleEndianInts(
                    Float.floatToRawIntBits(NEGATIVE_FLOAT_NAN),
                    Float.floatToRawIntBits(FLOAT_NAN_A))),
                2,
                Encoding.PLAIN);
          case FIXED_LEN_BYTE_ARRAY:
            return new DictionaryPage(
                BytesInput.from(new byte[] {(byte) 0x01, (byte) 0xfe, 0x01, 0x7e}), 2, Encoding.PLAIN);
          default:
            throw new AssertionError("Unexpected dictionary type: " + type);
        }
      }
    };
  }

  private static byte[] littleEndianLongs(long... values) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * values.length).order(ByteOrder.LITTLE_ENDIAN);
    for (long value : values) {
      buffer.putLong(value);
    }
    return buffer.array();
  }

  private static byte[] littleEndianInts(int... values) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * values.length).order(ByteOrder.LITTLE_ENDIAN);
    for (int value : values) {
      buffer.putInt(value);
    }
    return buffer.array();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testInBinary(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("binary_field");

    Set<Binary> set1 = new HashSet<>();
    set1.add(Binary.fromString("F"));
    set1.add(Binary.fromString("C"));
    set1.add(Binary.fromString("h"));
    set1.add(Binary.fromString("E"));
    FilterPredicate predIn1 = in(b, set1);
    FilterPredicate predNotIn1 = notIn(b, set1);
    assertThat(canDrop(predIn1, ccmd, dictionaries))
        .as("Should not drop block")
        .isFalse();
    assertThat(canDrop(predNotIn1, ccmd, dictionaries))
        .as("Should not drop block")
        .isFalse();

    Set<Binary> set2 = new HashSet<>();
    for (int i = 0; i < 26; i++) {
      set2.add(Binary.fromString(Character.toString((char) (i + 97))));
    }
    set2.add(Binary.fromString("A"));
    FilterPredicate predIn2 = in(b, set2);
    FilterPredicate predNotIn2 = notIn(b, set2);
    assertThat(canDrop(predIn2, ccmd, dictionaries))
        .as("Should not drop block")
        .isFalse();
    assertThat(canDrop(predNotIn2, ccmd, dictionaries))
        .as("Should not drop block")
        .isTrue();

    Set<Binary> set3 = new HashSet<>();
    set3.add(Binary.fromString("F"));
    set3.add(Binary.fromString("C"));
    set3.add(Binary.fromString("A"));
    set3.add(Binary.fromString("E"));
    FilterPredicate predIn3 = in(b, set3);
    FilterPredicate predNotIn3 = notIn(b, set3);
    assertThat(canDrop(predIn3, ccmd, dictionaries)).as("Should drop block").isTrue();
    assertThat(canDrop(predNotIn3, ccmd, dictionaries))
        .as("Should not drop block")
        .isFalse();

    Set<Binary> set4 = new HashSet<>();
    set4.add(null);
    FilterPredicate predIn4 = in(b, set4);
    FilterPredicate predNotIn4 = notIn(b, set4);
    assertThat(canDrop(predIn4, ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
    assertThat(canDrop(predNotIn4, ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();

    BinaryColumn sharpAndNull = binaryColumn("optional_single_value_field");

    // Test the case that all non-null values are in the set but the column may have nulls and the set has no nulls.
    Set<Binary> set5 = new HashSet<>();
    set5.add(Binary.fromString("sharp"));
    FilterPredicate predNotIn5 = notIn(sharpAndNull, set5);
    FilterPredicate predIn5 = in(sharpAndNull, set5);
    assertThat(canDrop(predNotIn5, ccmd, dictionaries))
        .as("Should not drop block")
        .isFalse();
    assertThat(canDrop(predIn5, ccmd, dictionaries))
        .as("Should not drop block")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testInFixed(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("fixed_field");

    // Only V2 supports dictionary encoding for FIXED_LEN_BYTE_ARRAY values
    if (version == PARQUET_2_0) {
      Set<Binary> set1 = new HashSet<>();
      set1.add(toBinary("-2", 17));
      set1.add(toBinary("-22", 17));
      set1.add(toBinary("12345", 17));
      FilterPredicate predIn1 = in(b, set1);
      FilterPredicate predNotIn1 = notIn(b, set1);
      assertThat(canDrop(predIn1, ccmd, dictionaries))
          .as("Should drop block for in (-2, -22, 12345)")
          .isTrue();
      assertThat(canDrop(predNotIn1, ccmd, dictionaries))
          .as("Should not drop block for notIn (-2, -22, 12345)")
          .isFalse();

      Set<Binary> set2 = new HashSet<>();
      set2.add(toBinary("-1", 17));
      set2.add(toBinary("0", 17));
      set2.add(toBinary("12345", 17));
      assertThat(canDrop(in(b, set2), ccmd, dictionaries))
          .as("Should not drop block for in (-1, 0, 12345)")
          .isFalse();
      assertThat(canDrop(notIn(b, set2), ccmd, dictionaries))
          .as("Should not drop block for in (-1, 0, 12345)")
          .isFalse();
    }

    Set<Binary> set3 = new HashSet<>();
    set3.add(null);
    FilterPredicate predIn3 = in(b, set3);
    FilterPredicate predNotIn3 = notIn(b, set3);
    assertThat(canDrop(predIn3, ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
    assertThat(canDrop(predNotIn3, ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testInInt96(WriterVersion version) throws Exception {
    setUp(version);
    // INT96 ordering is undefined => no filtering shall be done
    BinaryColumn b = binaryColumn("int96_field");

    Set<Binary> set1 = new HashSet<>();
    set1.add(toBinary("-2", 12));
    set1.add(toBinary("-0", 12));
    set1.add(toBinary("12345", 12));
    FilterPredicate predIn1 = in(b, set1);
    FilterPredicate predNotIn1 = notIn(b, set1);
    assertThat(canDrop(predIn1, ccmd, dictionaries))
        .as("Should not drop block for in (-2, -0, 12345)")
        .isFalse();
    assertThat(canDrop(predNotIn1, ccmd, dictionaries))
        .as("Should not drop block for notIn (-2, -0, 12345)")
        .isFalse();

    Set<Binary> set2 = new HashSet<>();
    set2.add(toBinary("-2", 17));
    set2.add(toBinary("12345", 17));
    set2.add(toBinary("-789", 17));
    FilterPredicate predIn2 = in(b, set2);
    FilterPredicate predNotIn2 = notIn(b, set2);
    assertThat(canDrop(predIn2, ccmd, dictionaries))
        .as("Should not drop block for in (-2, 12345, -789)")
        .isFalse();
    assertThat(canDrop(predNotIn2, ccmd, dictionaries))
        .as("Should not drop block for notIn (-2, 12345, -789)")
        .isFalse();

    Set<Binary> set3 = new HashSet<>();
    set3.add(null);
    FilterPredicate predIn3 = in(b, set3);
    FilterPredicate predNotIn3 = notIn(b, set3);
    assertThat(canDrop(predIn3, ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
    assertThat(canDrop(predNotIn3, ccmd, dictionaries))
        .as("Should not drop block for null")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testAnd(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    FilterPredicate B = eq(col, Binary.fromString("B"));
    FilterPredicate C = eq(col, Binary.fromString("C"));

    // both evaluate to true (all lower-case letters are in the dictionary)
    FilterPredicate x = eq(col, Binary.fromString("x"));
    FilterPredicate y = eq(col, Binary.fromString("y"));

    assertThat(canDrop(and(B, y), ccmd, dictionaries))
        .as("Should drop when either predicate must be false")
        .isTrue();
    assertThat(canDrop(and(x, C), ccmd, dictionaries))
        .as("Should drop when either predicate must be false")
        .isTrue();
    assertThat(canDrop(and(B, C), ccmd, dictionaries))
        .as("Should drop when either predicate must be false")
        .isTrue();
    assertThat(canDrop(and(x, y), ccmd, dictionaries))
        .as("Should not drop when either predicate could be true")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testOr(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    FilterPredicate B = eq(col, Binary.fromString("B"));
    FilterPredicate C = eq(col, Binary.fromString("C"));

    // both evaluate to true (all lower-case letters are in the dictionary)
    FilterPredicate x = eq(col, Binary.fromString("x"));
    FilterPredicate y = eq(col, Binary.fromString("y"));

    assertThat(canDrop(or(B, y), ccmd, dictionaries))
        .as("Should not drop when one predicate could be true")
        .isFalse();
    assertThat(canDrop(or(x, C), ccmd, dictionaries))
        .as("Should not drop when one predicate could be true")
        .isFalse();
    assertThat(canDrop(or(B, C), ccmd, dictionaries))
        .as("Should drop when both predicates must be false")
        .isTrue();
    assertThat(canDrop(or(x, y), ccmd, dictionaries))
        .as("Should not drop when one predicate could be true")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testUdp(WriterVersion version) throws Exception {
    setUp(version);
    InInt32UDP dropabble = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP undroppable = new InInt32UDP(ImmutableSet.of(205));

    assertThat(canDrop(userDefined(intColumn("int32_field"), dropabble), ccmd, dictionaries))
        .as("Should drop block for non-matching UDP")
        .isTrue();

    assertThat(canDrop(userDefined(intColumn("int32_field"), undroppable), ccmd, dictionaries))
        .as("Should not drop block for matching UDP")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testNullAcceptingUdp(WriterVersion version) throws Exception {
    setUp(version);
    InInt32UDP drop42DenyNulls = new InInt32UDP(Sets.newHashSet(205));
    InInt32UDP drop42AcceptNulls = new InInt32UDP(Sets.newHashSet(null, 205));

    // A column with value 42 and 10% nulls
    IntColumn intColumnWithNulls = intColumn("optional_single_value_int32_field");

    assertThat(canDrop(userDefined(intColumnWithNulls, drop42DenyNulls), ccmd, dictionaries))
        .as("Should drop block")
        .isTrue();
    assertThat(canDrop(userDefined(intColumnWithNulls, drop42AcceptNulls), ccmd, dictionaries))
        .as("Should not drop block for null accepting udp")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testInverseUdp(WriterVersion version) throws Exception {
    setUp(version);
    InInt32UDP droppable = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP undroppable = new InInt32UDP(ImmutableSet.of(205));
    Set<Integer> allValues = ImmutableSet.copyOf(Ints.asList(intValues));
    InInt32UDP completeMatch = new InInt32UDP(allValues);

    FilterPredicate inverse = LogicalInverseRewriter.rewrite(not(userDefined(intColumn("int32_field"), droppable)));
    FilterPredicate inverse1 =
        LogicalInverseRewriter.rewrite(not(userDefined(intColumn("int32_field"), undroppable)));
    FilterPredicate inverse2 =
        LogicalInverseRewriter.rewrite(not(userDefined(intColumn("int32_field"), completeMatch)));

    assertThat(canDrop(inverse, ccmd, dictionaries))
        .as("Should not drop block for inverse of non-matching UDP")
        .isFalse();

    assertThat(canDrop(inverse1, ccmd, dictionaries))
        .as("Should not drop block for inverse of UDP with some matches")
        .isFalse();

    assertThat(canDrop(inverse2, ccmd, dictionaries))
        .as("Should drop block for inverse of UDP with all matches")
        .isTrue();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testColumnWithoutDictionary(WriterVersion version) throws Exception {
    setUp(version);
    IntColumn plain = intColumn("plain_int32_field");
    DictionaryPageReadStore dictionaryStore = mock(DictionaryPageReadStore.class);

    assertThat(canDrop(eq(plain, -10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(lt(plain, -10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(ltEq(plain, -10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(gt(plain, nElements + 10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(gtEq(plain, nElements + 10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(notEq(plain, nElements + 10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    verifyNoInteractions(dictionaryStore);
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testColumnWithDictionaryAndPlainEncodings(WriterVersion version) throws Exception {
    setUp(version);
    IntColumn plain = intColumn("fallback_binary_field");
    DictionaryPageReadStore dictionaryStore = mock(DictionaryPageReadStore.class);

    assertThat(canDrop(eq(plain, -10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(lt(plain, -10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(ltEq(plain, -10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(gt(plain, nElements + 10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(gtEq(plain, nElements + 10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    assertThat(canDrop(notEq(plain, nElements + 10), ccmd, dictionaryStore))
        .as("Should never drop block using plain encoding")
        .isFalse();

    verifyNoInteractions(dictionaryStore);
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testEqMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("missing_column");

    assertThat(canDrop(eq(b, Binary.fromString("any")), ccmd, dictionaries))
        .as("Should drop block for non-null query")
        .isTrue();

    assertThat(canDrop(eq(b, null), ccmd, dictionaries))
        .as("Should not drop block null query")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testNotEqMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("missing_column");

    assertThat(canDrop(notEq(b, Binary.fromString("any")), ccmd, dictionaries))
        .as("Should not drop block for non-null query")
        .isFalse();

    assertThat(canDrop(notEq(b, null), ccmd, dictionaries))
        .as("Should not drop block null query")
        .isTrue();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testLtMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("missing_column");

    assertThat(canDrop(lt(b, Binary.fromString("any")), ccmd, dictionaries))
        .as("Should drop block for any non-null query")
        .isTrue();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testLtEqMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("missing_column");

    assertThat(canDrop(ltEq(b, Binary.fromString("any")), ccmd, dictionaries))
        .as("Should drop block for any non-null query")
        .isTrue();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testGtMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("missing_column");

    assertThat(canDrop(gt(b, Binary.fromString("any")), ccmd, dictionaries))
        .as("Should drop block for any non-null query")
        .isTrue();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testGtEqMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn b = binaryColumn("missing_column");

    assertThat(canDrop(gtEq(b, Binary.fromString("any")), ccmd, dictionaries))
        .as("Should drop block for any non-null query")
        .isTrue();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testUdpMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    InInt32UDP nullRejecting = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP nullAccepting = new InInt32UDP(Sets.newHashSet((Integer) null));
    IntColumn fake = intColumn("missing_column");

    assertThat(canDrop(userDefined(fake, nullRejecting), ccmd, dictionaries))
        .as("Should drop block for null rejecting udp")
        .isTrue();
    assertThat(canDrop(userDefined(fake, nullAccepting), ccmd, dictionaries))
        .as("Should not drop block for null accepting udp")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testInverseUdpMissingColumn(WriterVersion version) throws Exception {
    setUp(version);
    InInt32UDP nullRejecting = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP nullAccepting = new InInt32UDP(Sets.newHashSet((Integer) null));
    IntColumn fake = intColumn("missing_column");

    assertThat(canDrop(LogicalInverseRewriter.rewrite(not(userDefined(fake, nullAccepting))), ccmd, dictionaries))
        .as("Should drop block for null accepting udp")
        .isTrue();
    assertThat(canDrop(LogicalInverseRewriter.rewrite(not(userDefined(fake, nullRejecting))), ccmd, dictionaries))
        .as("Should not drop block for null rejecting udp")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testContainsAnd(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    Operators.Contains<Binary> B = contains(eq(col, Binary.fromString("B")));
    Operators.Contains<Binary> C = contains(eq(col, Binary.fromString("C")));

    // both evaluate to true (all lower-case letters are in the dictionary)
    Operators.Contains<Binary> x = contains(eq(col, Binary.fromString("x")));
    Operators.Contains<Binary> y = contains(eq(col, Binary.fromString("y")));

    assertThat(canDrop(and(B, y), ccmd, dictionaries))
        .as("Should drop when either predicate must be false")
        .isTrue();
    assertThat(canDrop(and(x, C), ccmd, dictionaries))
        .as("Should drop when either predicate must be false")
        .isTrue();
    assertThat(canDrop(and(B, C), ccmd, dictionaries))
        .as("Should drop when either predicate must be false")
        .isTrue();
    assertThat(canDrop(and(x, y), ccmd, dictionaries))
        .as("Should not drop when either predicate could be true")
        .isFalse();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void testContainsOr(WriterVersion version) throws Exception {
    setUp(version);
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    Operators.Contains<Binary> B = contains(eq(col, Binary.fromString("B")));
    Operators.Contains<Binary> C = contains(eq(col, Binary.fromString("C")));

    // both evaluate to true (all lower-case letters are in the dictionary)
    Operators.Contains<Binary> x = contains(eq(col, Binary.fromString("x")));
    Operators.Contains<Binary> y = contains(eq(col, Binary.fromString("y")));

    assertThat(canDrop(or(B, y), ccmd, dictionaries))
        .as("Should not drop when one predicate could be true")
        .isFalse();
    assertThat(canDrop(or(x, C), ccmd, dictionaries))
        .as("Should not drop when one predicate could be true")
        .isFalse();
    assertThat(canDrop(or(B, C), ccmd, dictionaries))
        .as("Should drop when both predicates must be false")
        .isTrue();
    assertThat(canDrop(or(x, y), ccmd, dictionaries))
        .as("Should not drop when one predicate could be true")
        .isFalse();
  }

  private static final class InInt32UDP extends UserDefinedPredicate<Integer> implements Serializable {

    private final Set<Integer> ints;

    InInt32UDP(Set<Integer> ints) {
      this.ints = ints;
    }

    @Override
    public boolean keep(Integer value) {
      return ints.contains(value);
    }

    @Override
    public boolean canDrop(Statistics<Integer> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Integer> statistics) {
      return false;
    }
  }

  private static double toDouble(int value) {
    return (value * 1.0);
  }

  private static float toFloat(int value) {
    return (float) (value * 2.0);
  }
}
