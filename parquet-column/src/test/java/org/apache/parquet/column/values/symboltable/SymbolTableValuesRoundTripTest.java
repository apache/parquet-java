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
package org.apache.parquet.column.values.symboltable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

/**
 * The writer and reader as a pair: values in, the same values out, over the seams that a symbol table
 * has to cross.
 *
 * <p>The table lives with the column chunk while a page is what a writer produces, and that mismatch
 * is where the interesting cases are — a table trained on the first page and used by the fifth, a
 * chunk that abandons the encoding after the table has already been trained, and a reader that has to
 * be handed the table from outside the page it is reading.
 */
public class SymbolTableValuesRoundTripTest {

  private static final int SLAB_SIZE = 1024;
  private static final int PAGE_SIZE = 1 << 20;

  /**
   * Stands in for wherever the serialized table ends up.
   *
   * <p>Both halves of the seam, so a test can hand the writer's own table straight back to the
   * reader. Deserializing on every call is deliberate: it means the reader is decoding against a table
   * rebuilt from bytes rather than against the trainer's own object.
   */
  private static final class SymbolTableRelay implements SymbolTableSink, SymbolTableSource {

    private SymbolTableType type;
    private byte[] body;
    private int publishCount;

    @Override
    public void putSymbolTable(SymbolTableType type, BytesInput body) {
      this.type = type;
      try {
        this.body = body.toByteArray();
      } catch (IOException e) {
        throw new AssertionError(e);
      }
      this.publishCount++;
    }

    @Override
    public SymbolTable getSymbolTable() {
      return SymbolTables.deserialize(type, body, 0, body.length);
    }
  }

  private static SymbolTableValuesWriter writer(SymbolTableSink sink, OffsetEncoding offsetEncoding) {
    return new SymbolTableValuesWriter(
        SymbolTableType.FSST_8,
        sink,
        offsetEncoding,
        SLAB_SIZE,
        PAGE_SIZE,
        HeapByteBufferAllocator.getInstance());
  }

  private static List<Binary> binaries(String... values) {
    List<Binary> result = new ArrayList<>();
    for (String value : values) {
      result.add(Binary.fromString(value));
    }
    return result;
  }

  /** Writes each page, reads each page back, and returns what came out. */
  private static List<List<Binary>> roundTrip(List<List<Binary>> pages, OffsetEncoding offsetEncoding)
      throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    List<byte[]> bodies = new ArrayList<>();
    try (SymbolTableValuesWriter writer = writer(relay, offsetEncoding)) {
      for (List<Binary> page : pages) {
        for (Binary value : page) {
          writer.writeBytes(value);
        }
        assertEquals(Encoding.FSST, writer.getEncoding());
        bodies.add(writer.getBytes().toByteArray());
        writer.reset();
      }
    }
    assertEquals("one table for the whole chunk", 1, relay.publishCount);

    SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
    List<List<Binary>> read = new ArrayList<>();
    for (int i = 0; i < pages.size(); i++) {
      int valueCount = pages.get(i).size();
      reader.initFromPage(valueCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(bodies.get(i))));
      List<Binary> page = new ArrayList<>();
      for (int v = 0; v < valueCount; v++) {
        page.add(reader.readBytes());
      }
      read.add(page);
    }
    return read;
  }

  private static void assertRoundTrips(List<Binary> values) throws IOException {
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      assertEquals(
          "offsets " + offsetEncoding,
          Arrays.asList(values),
          roundTrip(Arrays.asList(values), offsetEncoding));
    }
  }

  @Test
  public void roundTripsTextThatSharesSubstrings() throws IOException {
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      values.add(Binary.fromString("https://example.com/catalogue/item/" + i + "?ref=newsletter"));
    }
    assertRoundTrips(values);
  }

  @Test
  public void roundTripsAPageWithNoValues() throws IOException {
    assertRoundTrips(binaries());
  }

  @Test
  public void roundTripsASingleValue() throws IOException {
    assertRoundTrips(binaries("only"));
  }

  @Test
  public void roundTripsEmptyStrings() throws IOException {
    assertRoundTrips(binaries("", "", "", ""));
  }

  @Test
  public void roundTripsEmptyStringsMixedWithText() throws IOException {
    assertRoundTrips(binaries("", "alpha", "", "", "alphabet", "a", ""));
  }

  /** Every byte value, including the one the code space reserves for an escape. */
  @Test
  public void roundTripsAllTwoHundredAndFiftySixByteValues() throws IOException {
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      values.add(Binary.fromConstantByteArray(new byte[] {(byte) i}));
    }
    values.add(Binary.fromConstantByteArray(allByteValues()));
    assertRoundTrips(values);
  }

  /** High-entropy bytes, which is the input that forces the escape path to carry the page. */
  @Test
  public void roundTripsIncompressibleBytes() throws IOException {
    Random random = new Random(20260908L);
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      byte[] value = new byte[1 + random.nextInt(40)];
      random.nextBytes(value);
      values.add(Binary.fromConstantByteArray(value));
    }
    assertRoundTrips(values);
  }

  /** Values far longer than the longest symbol a table can hold. */
  @Test
  public void roundTripsValuesLongerThanAnySymbol() throws IOException {
    List<Binary> values = new ArrayList<>();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 400; i++) {
      builder.append("repetitionrepetition");
      values.add(Binary.fromString(builder.toString()));
    }
    assertRoundTrips(values);
  }

  /**
   * A table trained on the first page decodes the pages that follow it.
   *
   * <p>The pages deliberately drift: page one has none of the text page four is made of, so a table
   * retrained per page would give a smaller page four, and a reader given only the first page's table
   * would decode it wrongly. The point of the assertion is that the second thing does not happen.
   */
  @Test
  public void oneTableTrainedOnTheFirstPageServesLaterPages() throws IOException {
    List<List<Binary>> pages = new ArrayList<>();
    String[] themes = {"warehouse-inventory", "flight-departure", "clinical-observation", "seismic-reading"};
    for (int page = 0; page < themes.length; page++) {
      List<Binary> values = new ArrayList<>();
      for (int i = 0; i < 300; i++) {
        values.add(Binary.fromString(themes[page] + "/" + i));
      }
      pages.add(values);
    }
    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      assertEquals("offsets " + offsetEncoding, pages, roundTrip(pages, offsetEncoding));
    }
  }

  /** A new chunk trains a new table, which is what a row group boundary needs. */
  @Test
  public void resetDictionaryTrainsAgain() throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    try (SymbolTableValuesWriter writer = writer(relay, OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (Binary value : binaries("alpha", "alphabet", "alpine")) {
        writer.writeBytes(value);
      }
      writer.getBytes();
      writer.reset();
      assertEquals(1, relay.publishCount);

      writer.resetDictionary();
      for (Binary value : binaries("zeta", "zenith", "zephyr")) {
        writer.writeBytes(value);
      }
      writer.getBytes();
      assertEquals(2, relay.publishCount);
    }
  }

  @Test
  public void skipReachesTheSameValuesAsReading() throws IOException {
    List<Binary> values = binaries("alpha", "", "alphabet", "beta", "betamax", "gamma", "gamma-ray", "delta");
    SymbolTableRelay relay = new SymbolTableRelay();
    byte[] body;
    try (SymbolTableValuesWriter writer = writer(relay, OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (Binary value : values) {
        writer.writeBytes(value);
      }
      body = writer.getBytes().toByteArray();
    }

    for (int start = 0; start < values.size(); start++) {
      SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
      reader.initFromPage(values.size(), ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
      reader.skip(start);
      for (int i = start; i < values.size(); i++) {
        assertEquals("from " + start + " at " + i, values.get(i), reader.readBytes());
      }
    }

    // One value at a time, which is the overload a column reader calls for a null.
    SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
    reader.initFromPage(values.size(), ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
    for (int i = 0; i < values.size(); i++) {
      if (i % 2 == 0) {
        reader.skip();
      } else {
        assertEquals(values.get(i), reader.readBytes());
      }
    }
  }

  @Test
  public void readingPastTheEndOfAPageIsRejected() throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    byte[] body;
    try (SymbolTableValuesWriter writer = writer(relay, OffsetEncoding.DELTA_BINARY_PACKED)) {
      writer.writeBytes(Binary.fromString("one"));
      body = writer.getBytes().toByteArray();
    }
    SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
    reader.readBytes();
    assertThrows(ParquetDecodingException.class, reader::readBytes);
    assertThrows(ParquetDecodingException.class, reader::skip);
  }

  @Test
  public void aReaderWithNoTableSaysSoRatherThanFailingLater() {
    SymbolTableValuesReader reader = new SymbolTableValuesReader();
    assertNull(reader.symbolTableType());
    ParquetDecodingException thrown = assertThrows(
        ParquetDecodingException.class,
        () -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(new byte[9]))));
    assertTrue(thrown.getMessage().contains("#531"));
  }

  @Test
  public void aWriterWithNowhereToPutItsTableSaysSoBeforeWritingAPage() throws IOException {
    // What a writer built from the format's own settings gets, because the format has no place for a
    // symbol table. Refusing at the first page beats writing pages nothing can decode.
    try (SymbolTableValuesWriter writer = new SymbolTableValuesWriter(
        SymbolTableType.FSST_8,
        SymbolTables.rejectingSink(),
        OffsetEncoding.DELTA_BINARY_PACKED,
        SLAB_SIZE,
        PAGE_SIZE,
        HeapByteBufferAllocator.getInstance())) {
      writer.writeBytes(Binary.fromString("http://example.com/a"));
      ParquetEncodingException thrown = assertThrows(ParquetEncodingException.class, writer::getBytes);
      assertTrue(thrown.getMessage().contains("#531"));
    }
  }

  @Test
  public void theEncodingHandsOutTheReaderForBinaryOnly() {
    assertTrue(
        Encoding.FSST.getValuesReader(descriptor(PrimitiveTypeName.BINARY), ValuesType.VALUES)
            instanceof SymbolTableValuesReader);
    for (PrimitiveTypeName type : new PrimitiveTypeName[] {
      PrimitiveTypeName.INT32, PrimitiveTypeName.INT64, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
    }) {
      assertThrows(
          ParquetDecodingException.class,
          () -> Encoding.FSST.getValuesReader(descriptor(type), ValuesType.VALUES));
    }
  }

  /**
   * A page the encoding makes bigger is written plain instead, and the values still come back.
   *
   * <p>Single bytes with four-byte offsets: the codes are as long as the values and the offsets cost
   * what a plain page's lengths cost, so the nine-byte header alone decides it. That is the
   * configuration to fall back from, and it is also the argument against writing offsets plain — the
   * same page with packed offsets is smaller than plain and keeps the encoding.
   */
  @Test
  public void aPageTheEncodingWouldGrowIsWrittenPlain() throws IOException {
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      values.add(Binary.fromConstantByteArray(new byte[] {(byte) i}));
    }

    assertEquals(Encoding.PLAIN, fallbackEncodingFor(values, OffsetEncoding.PLAIN));
    assertEquals(Encoding.FSST, fallbackEncodingFor(values, OffsetEncoding.DELTA_BINARY_PACKED));
  }

  /** Runs a page through the fallback wrapper and reads it back with whatever encoding it chose. */
  private static Encoding fallbackEncodingFor(List<Binary> values, OffsetEncoding offsetEncoding) throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    Encoding encoding;
    byte[] body;
    try (FallbackValuesWriter<SymbolTableValuesWriter, PlainValuesWriter> writer = FallbackValuesWriter.of(
        writer(relay, offsetEncoding),
        new PlainValuesWriter(SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator.getInstance()))) {
      for (Binary value : values) {
        writer.writeBytes(value);
      }
      body = writer.getBytes().toByteArray();
      encoding = writer.getEncoding();
    }

    ValuesReader reader =
        encoding == Encoding.PLAIN ? new BinaryPlainValuesReader() : new SymbolTableValuesReader(relay);
    reader.initFromPage(values.size(), ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
    for (Binary value : values) {
      assertEquals(value, reader.readBytes());
    }
    return encoding;
  }

  /** Replaying buffered values into another writer must not depend on the encoding having run. */
  @Test
  public void fallingBackReplaysEveryValue() throws IOException {
    List<Binary> values = binaries("alpha", "", "beta", "gamma");
    SymbolTableRelay relay = new SymbolTableRelay();
    List<Binary> replayed = new ArrayList<>();
    ValuesWriter collector = new CollectingValuesWriter(replayed);
    try (SymbolTableValuesWriter writer = writer(relay, OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (Binary value : values) {
        writer.writeBytes(value);
      }
      writer.fallBackAllValuesTo(collector);
    }
    assertEquals(values, replayed);
    assertEquals(0, relay.publishCount);
  }

  @Test
  public void aTableIsPublishedOnceAndRebuiltFromItsBytes() throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    try (SymbolTableValuesWriter writer = writer(relay, OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (int i = 0; i < 200; i++) {
        writer.writeBytes(Binary.fromString("measurement-" + i));
      }
      writer.getBytes();
    }
    assertEquals(SymbolTableType.FSST_8, relay.type);
    SymbolTable first = relay.getSymbolTable();
    assertEquals(SymbolTableType.FSST_8, first.type());
    assertTrue("a table was trained", first.symbolCount() > 0);
    assertEquals(first.symbolCount(), relay.getSymbolTable().symbolCount());
  }

  @Test
  public void theWriterReportsWhatItIsHoldingAndReleasesItOnReset() throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    try (SymbolTableValuesWriter writer = writer(relay, OffsetEncoding.DELTA_BINARY_PACKED)) {
      assertEquals(0, writer.getBufferedSize());
      writer.writeBytes(Binary.fromString("alpha"));
      assertEquals(5 + 4, writer.getBufferedSize());
      writer.getBytes();
      writer.reset();
      assertEquals(0, writer.getBufferedSize());
      assertTrue(writer.getAllocatedSize() > 0);
      assertTrue(writer.memUsageString("x").startsWith("x "));
      assertEquals(SymbolTableType.FSST_8, writer.symbolTableType());
      // The only fallback question is asked once, and never by the writer itself.
      assertTrue(writer.isCompressionSatisfying(100, 99));
      assertFalse(writer.isCompressionSatisfying(100, 100));
      assertFalse(writer.shouldFallBack());
    }
  }

  private static byte[] allByteValues() {
    byte[] all = new byte[256];
    for (int i = 0; i < all.length; i++) {
      all[i] = (byte) i;
    }
    return all;
  }

  private static ColumnDescriptor descriptor(PrimitiveTypeName type) {
    return new ColumnDescriptor(new String[] {"column"}, type, 0, 0);
  }

  /** Records what a fallback replay hands it, so the replay can be compared value by value. */
  private static final class CollectingValuesWriter extends ValuesWriter {

    private final List<Binary> values;

    CollectingValuesWriter(List<Binary> values) {
      this.values = values;
    }

    @Override
    public void writeBytes(Binary v) {
      // Copy, because a replay is allowed to hand over a view of a buffer it will reuse.
      values.add(Binary.fromString(new String(v.getBytes(), StandardCharsets.UTF_8)));
    }

    @Override
    public long getBufferedSize() {
      return 0;
    }

    @Override
    public BytesInput getBytes() {
      return BytesInput.empty();
    }

    @Override
    public Encoding getEncoding() {
      return Encoding.PLAIN;
    }

    @Override
    public void reset() {
      values.clear();
    }

    @Override
    public long getAllocatedSize() {
      return 0;
    }

    @Override
    public String memUsageString(String prefix) {
      return prefix;
    }
  }
}
