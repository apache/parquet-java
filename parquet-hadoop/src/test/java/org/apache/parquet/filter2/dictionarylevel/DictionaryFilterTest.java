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

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.LogicalInverseRewriter;
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
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.filter2.dictionarylevel.DictionaryFilter.canDrop;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

public class DictionaryFilterTest {

  private static final int nElements = 1000;
  private static final Configuration conf = new Configuration();
  private static  Path file = new Path("target/test/TestDictionaryFilter/testParquetFile");
  private static final MessageType schema = parseMessageType(
      "message test { "
          + "required binary binary_field; "
          + "required binary single_value_field; "
          + "required int32 int32_field; "
          + "required int64 int64_field; "
          + "required double double_field; "
          + "required float float_field; "
          + "required int32 plain_int32_field; "
          + "required binary fallback_binary_field; "
          + "} ");

  private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
  private static final int[] intValues = new int[] {
      -100, 302, 3333333, 7654321, 1234567, -2000, -77775, 0, 75, 22223,
      77, 22221, -444443, 205, 12, 44444, 889, 66665, -777889, -7,
      52, 33, -257, 1111, 775, 26};
  private static final long[] longValues = new long[] {
      -100L, 302L, 3333333L, 7654321L, 1234567L, -2000L, -77775L, 0L,
      75L, 22223L, 77L, 22221L, -444443L, 205L, 12L, 44444L, 889L, 66665L,
      -777889L, -7L, 52L, 33L, -257L, 1111L, 775L, 26L};

  private static void writeData(SimpleGroupFactory f, ParquetWriter<Group> writer) throws IOException {
    for (int i = 0; i < nElements; i++) {
      int index = i % ALPHABET.length();

      Group group = f.newGroup()
          .append("binary_field", ALPHABET.substring(index, index+1))
          .append("single_value_field", "sharp")
          .append("int32_field", intValues[i % intValues.length])
          .append("int64_field", longValues[i % longValues.length])
          .append("double_field", toDouble(intValues[i % intValues.length]))
          .append("float_field", toFloat(intValues[i % intValues.length]))
          .append("plain_int32_field", i)
          .append("fallback_binary_field", i < (nElements / 2) ?
              ALPHABET.substring(index, index+1) : UUID.randomUUID().toString());

      writer.write(group);
    }
    writer.close();
  }

  @BeforeClass
  public static void prepareFile() throws IOException {
    cleanup();

    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
        .withWriterVersion(PARQUET_1_0)
        .withCompressionCodec(GZIP)
        .withRowGroupSize(1024*1024)
        .withPageSize(1024)
        .enableDictionaryEncoding()
        .withDictionaryPageSize(2*1024)
        .withConf(conf)
        .build();
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
  ParquetFileReader reader;
  DictionaryPageReadStore dictionaries;

  @Before
  public void setUp() throws Exception {
    reader = ParquetFileReader.open(conf, file);
    ParquetMetadata meta = reader.getFooter();
    ccmd = meta.getBlocks().get(0).getColumns();
    dictionaries = reader.getDictionaryReader(meta.getBlocks().get(0));
  }

  @After
  public void tearDown() throws Exception {
    reader.close();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testDictionaryEncodedColumns() throws Exception {
    Set<String> dictionaryEncodedColumns = new HashSet<String>(Arrays.asList(
        "binary_field", "single_value_field", "int32_field", "int64_field",
        "double_field", "float_field"));
    for (ColumnChunkMetaData column : ccmd) {
      String name = column.getPath().toDotString();
      if (dictionaryEncodedColumns.contains(name)) {
        assertTrue("Column should be dictionary encoded: " + name,
            column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
        assertFalse("Column should not have plain data pages" + name,
            column.getEncodings().contains(Encoding.PLAIN));

      } else {
        assertTrue("Column should have plain encoding: " + name,
            column.getEncodings().contains(Encoding.PLAIN));

        if (name.startsWith("fallback")) {
          assertTrue("Column should be have some dictionary encoding: " + name,
              column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
        } else {
          assertFalse("Column should have no dictionary encoding: " + name,
              column.getEncodings().contains(Encoding.PLAIN_DICTIONARY));
        }
      }
    }
  }

  @Test
  public void testEqBinary() throws Exception {
    BinaryColumn b = binaryColumn("binary_field");
    FilterPredicate pred = eq(b, Binary.fromString("c"));

    assertFalse("Should not drop block for lower case letters",
        canDrop(pred, ccmd, dictionaries));

    assertTrue("Should drop block for upper case letters",
        canDrop(eq(b, Binary.fromString("A")), ccmd, dictionaries));

    assertFalse("Should not drop block for null",
        canDrop(eq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testNotEqBinary() throws Exception {
    BinaryColumn sharp = binaryColumn("single_value_field");
    BinaryColumn b = binaryColumn("binary_field");

    assertTrue("Should drop block with only the excluded value",
        canDrop(notEq(sharp, Binary.fromString("sharp")), ccmd, dictionaries));

    assertFalse("Should not drop block with any other value",
        canDrop(notEq(sharp, Binary.fromString("applause")), ccmd, dictionaries));

    assertFalse("Should not drop block with a known value",
        canDrop(notEq(b, Binary.fromString("x")), ccmd, dictionaries));

    assertFalse("Should not drop block with a known value",
        canDrop(notEq(b, Binary.fromString("B")), ccmd, dictionaries));

    assertFalse("Should not drop block for null",
        canDrop(notEq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testLtInt() throws Exception {
    IntColumn i32 = intColumn("int32_field");
    int lowest = Integer.MAX_VALUE;
    for (int value : intValues) {
      lowest = Math.min(lowest, value);
    }

    assertTrue("Should drop: < lowest value",
        canDrop(lt(i32, lowest), ccmd, dictionaries));
    assertFalse("Should not drop: < (lowest value + 1)",
        canDrop(lt(i32, lowest + 1), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(lt(i32, Integer.MAX_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testLtEqLong() throws Exception {
    LongColumn i64 = longColumn("int64_field");
    long lowest = Long.MAX_VALUE;
    for (long value : longValues) {
      lowest = Math.min(lowest, value);
    }

    assertTrue("Should drop: <= lowest - 1",
        canDrop(ltEq(i64, lowest - 1), ccmd, dictionaries));
    assertFalse("Should not drop: <= lowest",
        canDrop(ltEq(i64, lowest), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(ltEq(i64, Long.MAX_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testGtFloat() throws Exception {
    FloatColumn f = floatColumn("float_field");
    float highest = Float.MIN_VALUE;
    for (int value : intValues) {
      highest = Math.max(highest, toFloat(value));
    }

    assertTrue("Should drop: > highest value",
        canDrop(gt(f, highest), ccmd, dictionaries));
    assertFalse("Should not drop: > (highest value - 1.0)",
        canDrop(gt(f, highest - 1.0f), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(gt(f, Float.MIN_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testGtEqDouble() throws Exception {
    DoubleColumn d = doubleColumn("double_field");
    double highest = Double.MIN_VALUE;
    for (int value : intValues) {
      highest = Math.max(highest, toDouble(value));
    }

    assertTrue("Should drop: >= highest + 0.00000001",
        canDrop(gtEq(d, highest + 0.00000001), ccmd, dictionaries));
    assertFalse("Should not drop: >= highest",
        canDrop(gtEq(d, highest), ccmd, dictionaries));

    assertFalse("Should not drop: contains matching values",
        canDrop(gtEq(d, Double.MIN_VALUE), ccmd, dictionaries));
  }

  @Test
  public void testAnd() throws Exception {
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    FilterPredicate B = eq(col, Binary.fromString("B"));
    FilterPredicate C = eq(col, Binary.fromString("C"));

    // both evaluate to true (all lower-case letters are in the dictionary)
    FilterPredicate x = eq(col, Binary.fromString("x"));
    FilterPredicate y = eq(col, Binary.fromString("y"));

    assertTrue("Should drop when either predicate must be false",
        canDrop(and(B, y), ccmd, dictionaries));
    assertTrue("Should drop when either predicate must be false",
        canDrop(and(x, C), ccmd, dictionaries));
    assertTrue("Should drop when either predicate must be false",
        canDrop(and(B, C), ccmd, dictionaries));
    assertFalse("Should not drop when either predicate could be true",
        canDrop(and(x, y), ccmd, dictionaries));
  }

  @Test
  public void testOr() throws Exception {
    BinaryColumn col = binaryColumn("binary_field");

    // both evaluate to false (no upper-case letters are in the dictionary)
    FilterPredicate B = eq(col, Binary.fromString("B"));
    FilterPredicate C = eq(col, Binary.fromString("C"));

    // both evaluate to true (all lower-case letters are in the dictionary)
    FilterPredicate x = eq(col, Binary.fromString("x"));
    FilterPredicate y = eq(col, Binary.fromString("y"));

    assertFalse("Should not drop when one predicate could be true",
        canDrop(or(B, y), ccmd, dictionaries));
    assertFalse("Should not drop when one predicate could be true",
        canDrop(or(x, C), ccmd, dictionaries));
    assertTrue("Should drop when both predicates must be false",
        canDrop(or(B, C), ccmd, dictionaries));
    assertFalse("Should not drop when one predicate could be true",
        canDrop(or(x, y), ccmd, dictionaries));
  }


  @Test
  public void testUdp() throws Exception {
    InInt32UDP udp = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP udp1 = new InInt32UDP(ImmutableSet.of(205));

    assertTrue("Should drop block for non-matching UDP",
      canDrop(FilterApi.userDefined(intColumn("int32_field"), udp), ccmd, dictionaries));

    assertFalse("Should not drop block for matching UDP",
      canDrop(FilterApi.userDefined(intColumn("int32_field"), udp1), ccmd, dictionaries));
  }

  @Test
  public void testInverseUdp() throws Exception {
    InInt32UDP udp = new InInt32UDP(ImmutableSet.of(42));
    InInt32UDP udp1 = new InInt32UDP(ImmutableSet.of(205));
    Set<Integer> allValues = ImmutableSet.copyOf(Arrays.asList(ArrayUtils.toObject(intValues)));
    InInt32UDP udp2 = new InInt32UDP(allValues);

    FilterPredicate inverse =
      LogicalInverseRewriter.rewrite(FilterApi.not(FilterApi.userDefined(intColumn("int32_field"), udp)));
    FilterPredicate inverse1 =
      LogicalInverseRewriter.rewrite(FilterApi.not(FilterApi.userDefined(intColumn("int32_field"), udp1)));
    FilterPredicate inverse2 =
      LogicalInverseRewriter.rewrite(FilterApi.not(FilterApi.userDefined(intColumn("int32_field"), udp2)));

    assertFalse("Should not drop block for inverse of non-matching UDP",
      canDrop(inverse, ccmd, dictionaries));

    assertFalse("Should not drop block for inverse of UDP with some matches",
      canDrop(inverse1, ccmd, dictionaries));

    assertTrue("Should drop block for inverse of UDP with all matches",
      canDrop(inverse2, ccmd, dictionaries));
  }

  @Test
  public void testColumnWithoutDictionary() throws Exception {
    IntColumn plain = intColumn("plain_int32_field");
    DictionaryPageReadStore dictionaryStore = mock(DictionaryPageReadStore.class);

    assertFalse("Should never drop block using plain encoding",
        canDrop(eq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(lt(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(ltEq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gt(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gtEq(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(notEq(plain, nElements + 10), ccmd, dictionaryStore));

    verifyZeroInteractions(dictionaryStore);
  }

  @Test
  public void testColumnWithDictionaryAndPlainEncodings() throws Exception {
    IntColumn plain = intColumn("fallback_binary_field");
    DictionaryPageReadStore dictionaryStore = mock(DictionaryPageReadStore.class);

    assertFalse("Should never drop block using plain encoding",
        canDrop(eq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(lt(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(ltEq(plain, -10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gt(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(gtEq(plain, nElements + 10), ccmd, dictionaryStore));

    assertFalse("Should never drop block using plain encoding",
        canDrop(notEq(plain, nElements + 10), ccmd, dictionaryStore));

    verifyZeroInteractions(dictionaryStore);
  }

  @Test
  public void testEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for non-null query",
        canDrop(eq(b, Binary.fromString("any")), ccmd, dictionaries));

    assertFalse("Should not drop block null query",
        canDrop(eq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testNotEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertFalse("Should not drop block for non-null query",
        canDrop(notEq(b, Binary.fromString("any")), ccmd, dictionaries));

    assertTrue("Should not drop block null query",
        canDrop(notEq(b, null), ccmd, dictionaries));
  }

  @Test
  public void testLtMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(lt(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testLtEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(ltEq(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testGtMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(gt(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testGtEqMissingColumn() throws Exception {
    BinaryColumn b = binaryColumn("missing_column");

    assertTrue("Should drop block for any non-null query",
        canDrop(gtEq(b, Binary.fromString("any")), ccmd, dictionaries));
  }

  @Test
  public void testUdpMissingColumn() throws Exception {
    InInt32UDP udp = new InInt32UDP(ImmutableSet.of(42));
    IntColumn fake = intColumn("missing_column");

    assertFalse("Should not drop block for any null columns",
      canDrop(FilterApi.userDefined(fake, udp), ccmd, dictionaries));
  }


  @Test
  public void testInverseUdpMissingColumn() throws Exception {
    InInt32UDP udp = new InInt32UDP(ImmutableSet.of(42));
    IntColumn fake = intColumn("missing_column");

    assertFalse("Should not drop block for any null columns",
      canDrop(LogicalInverseRewriter.rewrite(FilterApi.userDefined(fake, udp)), ccmd, dictionaries));
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
