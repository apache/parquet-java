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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.apache.parquet.column.page.SymbolTablePage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.fallback.FallbackValuesWriter;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

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
   * Stands in for wherever the deserialized {@link SymbolTablePage} ends up.
   *
   * <p>Deserializing on every {@link #getSymbolTable()} call is deliberate: it means the reader is
   * decoding against a table rebuilt from bytes rather than against the trainer's own object.
   */
  private static final class SymbolTableRelay implements SymbolTableSource {

    private SymbolTableType type;
    private byte[] body;

    void receive(SymbolTablePage page) throws IOException {
      this.type = page.getType();
      this.body = page.getBytes().toByteArray();
    }

    @Override
    public SymbolTable getSymbolTable() {
      return SymbolTables.deserialize(type, body, 0, body.length);
    }
  }

  private static SymbolTableValuesWriter writer(OffsetEncoding offsetEncoding) {
    return new SymbolTableValuesWriter(
        SymbolTableType.FSST_8, offsetEncoding, SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator.getInstance());
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
    try (SymbolTableValuesWriter writer = writer(offsetEncoding)) {
      for (List<Binary> page : pages) {
        for (Binary value : page) {
          writer.writeBytes(value);
        }
        assertThat(writer.getEncoding()).isEqualTo(Encoding.FSST);
        bodies.add(writer.getBytes().toByteArray());
        writer.reset();
      }
      SymbolTablePage tablePage = writer.toSymbolTablePageAndClose();
      assertThat(tablePage).as("one table for the whole chunk").isNotNull();
      relay.receive(tablePage);
    }

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
      assertThat(roundTrip(Arrays.asList(values), offsetEncoding))
          .as("offsets " + offsetEncoding)
          .isEqualTo(Arrays.asList(values));
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
      assertThat(roundTrip(pages, offsetEncoding))
          .as("offsets " + offsetEncoding)
          .isEqualTo(pages);
    }
  }

  /** A new chunk trains a new table, which is what a row group boundary needs. */
  @Test
  public void resetDictionaryTrainsAgain() throws IOException {
    try (SymbolTableValuesWriter writer = writer(OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (Binary value : binaries("alpha", "alphabet", "alpine")) {
        writer.writeBytes(value);
      }
      writer.getBytes();
      writer.reset();
      SymbolTablePage first = writer.toSymbolTablePageAndClose();
      assertThat(first).as("the first chunk trained a table").isNotNull();

      writer.resetDictionary();
      assertThat(writer.toSymbolTablePageAndClose())
          .as("nothing has been trained yet for the new chunk")
          .isNull();

      for (Binary value : binaries("zeta", "zenith", "zephyr")) {
        writer.writeBytes(value);
      }
      writer.getBytes();
      SymbolTablePage second = writer.toSymbolTablePageAndClose();
      assertThat(second).as("the second chunk trained its own table").isNotNull();
      assertThat(second.getBytes().toByteArray())
          .as("a table trained on different values")
          .isNotEqualTo(first.getBytes().toByteArray());
    }
  }

  @Test
  public void skipReachesTheSameValuesAsReading() throws IOException {
    List<Binary> values = binaries("alpha", "", "alphabet", "beta", "betamax", "gamma", "gamma-ray", "delta");
    SymbolTableRelay relay = new SymbolTableRelay();
    byte[] body;
    try (SymbolTableValuesWriter writer = writer(OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (Binary value : values) {
        writer.writeBytes(value);
      }
      body = writer.getBytes().toByteArray();
      relay.receive(writer.toSymbolTablePageAndClose());
    }

    for (int start = 0; start < values.size(); start++) {
      SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
      reader.initFromPage(values.size(), ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
      reader.skip(start);
      for (int i = start; i < values.size(); i++) {
        assertThat(reader.readBytes()).as("from " + start + " at " + i).isEqualTo(values.get(i));
      }
    }

    // One value at a time, which is the overload a column reader calls for a null.
    SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
    reader.initFromPage(values.size(), ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
    for (int i = 0; i < values.size(); i++) {
      if (i % 2 == 0) {
        reader.skip();
      } else {
        assertThat(reader.readBytes()).isEqualTo(values.get(i));
      }
    }
  }

  @Test
  public void readingPastTheEndOfAPageIsRejected() throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    byte[] body;
    try (SymbolTableValuesWriter writer = writer(OffsetEncoding.DELTA_BINARY_PACKED)) {
      writer.writeBytes(Binary.fromString("one"));
      body = writer.getBytes().toByteArray();
      relay.receive(writer.toSymbolTablePageAndClose());
    }
    SymbolTableValuesReader reader = new SymbolTableValuesReader(relay);
    reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
    reader.readBytes();
    assertThatThrownBy(reader::readBytes).isInstanceOf(ParquetDecodingException.class);
    assertThatThrownBy(reader::skip).isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void aReaderWithNoTableSaysSoRatherThanFailingLater() {
    SymbolTableValuesReader reader = new SymbolTableValuesReader();
    assertThat(reader.symbolTableType()).isNull();
    assertThatThrownBy(() -> reader.initFromPage(1, ByteBufferInputStream.wrap(ByteBuffer.wrap(new byte[9]))))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessageContaining("#531");
  }

  @Test
  public void theEncodingHandsOutTheReaderForBinaryOnly() {
    assertThat(Encoding.FSST.getValuesReader(descriptor(PrimitiveTypeName.BINARY), ValuesType.VALUES))
        .isInstanceOf(SymbolTableValuesReader.class);
    for (PrimitiveTypeName type : new PrimitiveTypeName[] {
      PrimitiveTypeName.INT32, PrimitiveTypeName.INT64, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
    }) {
      assertThatThrownBy(() -> Encoding.FSST.getValuesReader(descriptor(type), ValuesType.VALUES))
          .isInstanceOf(ParquetDecodingException.class);
    }
  }

  /**
   * A page the encoding makes bigger is written plain instead, and the values still come back.
   *
   * <p>Single bytes with four-byte offsets: the codes are as long as the values and the offsets cost
   * what a plain page's lengths cost, so the nine-byte header alone decides it. That is the
   * configuration to fall back from, and it is also the argument against writing offsets plain — the
   * same page with packed offsets is smaller than plain and keeps the encoding.
   *
   * <p>Also the regression test for a fixed publish-timing defect: a chunk that falls back must not
   * leave a symbol table page behind that no page refers to.
   */
  @Test
  public void aPageTheEncodingWouldGrowIsWrittenPlain() throws IOException {
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      values.add(Binary.fromConstantByteArray(new byte[] {(byte) i}));
    }

    assertThat(fallbackEncodingFor(values, OffsetEncoding.PLAIN)).isEqualTo(Encoding.PLAIN);
    assertThat(fallbackEncodingFor(values, OffsetEncoding.DELTA_BINARY_PACKED))
        .isEqualTo(Encoding.FSST);
  }

  /**
   * Runs a page through the fallback wrapper, reads it back with whatever encoding it chose, and
   * checks that a symbol table page was published if and only if the encoding kept FSST.
   */
  private static Encoding fallbackEncodingFor(List<Binary> values, OffsetEncoding offsetEncoding) throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    Encoding encoding;
    byte[] body;
    SymbolTablePage tablePage;
    try (FallbackValuesWriter<SymbolTableValuesWriter, PlainValuesWriter> writer = FallbackValuesWriter.of(
        writer(offsetEncoding),
        new PlainValuesWriter(SLAB_SIZE, PAGE_SIZE, HeapByteBufferAllocator.getInstance()))) {
      for (Binary value : values) {
        writer.writeBytes(value);
      }
      body = writer.getBytes().toByteArray();
      encoding = writer.getEncoding();
      tablePage = writer.toSymbolTablePageAndClose();
    }

    if (encoding == Encoding.PLAIN) {
      assertThat(tablePage)
          .as("a chunk that fell back publishes no table")
          .isNull();
    } else {
      assertThat(tablePage)
          .as("a chunk that kept the encoding publishes its table")
          .isNotNull();
      relay.receive(tablePage);
    }

    ValuesReader reader =
        encoding == Encoding.PLAIN ? new BinaryPlainValuesReader() : new SymbolTableValuesReader(relay);
    reader.initFromPage(values.size(), ByteBufferInputStream.wrap(ByteBuffer.wrap(body)));
    for (Binary value : values) {
      assertThat(reader.readBytes()).isEqualTo(value);
    }
    return encoding;
  }

  /** Replaying buffered values into another writer must not depend on the encoding having run. */
  @Test
  public void fallingBackReplaysEveryValue() throws IOException {
    List<Binary> values = binaries("alpha", "", "beta", "gamma");
    List<Binary> replayed = new ArrayList<>();
    ValuesWriter collector = new CollectingValuesWriter(replayed);
    try (SymbolTableValuesWriter writer = writer(OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (Binary value : values) {
        writer.writeBytes(value);
      }
      writer.fallBackAllValuesTo(collector);
    }
    assertThat(replayed).isEqualTo(values);
  }

  @Test
  public void aTableIsPublishedOnceAndRebuiltFromItsBytes() throws IOException {
    SymbolTableRelay relay = new SymbolTableRelay();
    SymbolTablePage tablePage;
    try (SymbolTableValuesWriter writer = writer(OffsetEncoding.DELTA_BINARY_PACKED)) {
      for (int i = 0; i < 200; i++) {
        writer.writeBytes(Binary.fromString("measurement-" + i));
      }
      writer.getBytes();
      tablePage = writer.toSymbolTablePageAndClose();
    }
    assertThat(tablePage.getType()).isEqualTo(SymbolTableType.FSST_8);
    relay.receive(tablePage);
    SymbolTable first = relay.getSymbolTable();
    assertThat(first.type()).isEqualTo(SymbolTableType.FSST_8);
    assertThat(first.symbolCount()).as("a table was trained").isPositive();
    assertThat(relay.getSymbolTable().symbolCount()).isEqualTo(first.symbolCount());
  }

  @Test
  public void theWriterReportsWhatItIsHoldingAndReleasesItOnReset() throws IOException {
    try (SymbolTableValuesWriter writer = writer(OffsetEncoding.DELTA_BINARY_PACKED)) {
      assertThat(writer.getBufferedSize()).isEqualTo(0);
      writer.writeBytes(Binary.fromString("alpha"));
      assertThat(writer.getBufferedSize()).isEqualTo(5 + 4);
      writer.getBytes();
      writer.reset();
      assertThat(writer.getBufferedSize()).isEqualTo(0);
      assertThat(writer.getAllocatedSize()).isPositive();
      assertThat(writer.memUsageString("x")).startsWith("x ");
      assertThat(writer.symbolTableType()).isEqualTo(SymbolTableType.FSST_8);
      // The only fallback question is asked once, and never by the writer itself.
      assertThat(writer.isCompressionSatisfying(100, 99)).isTrue();
      assertThat(writer.isCompressionSatisfying(100, 100)).isFalse();
      assertThat(writer.shouldFallBack()).isFalse();
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
