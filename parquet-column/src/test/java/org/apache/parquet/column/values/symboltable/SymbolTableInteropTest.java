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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

/**
 * Requires this implementation to decode symbol table pages another implementation wrote.
 *
 * <p>The round trip and reference comparison tests in this package both start from bytes this code
 * produced: one proves the decoder undoes the encoder, the other proves the trainer picks the same
 * symbols as the reference implementation. Neither can catch a page layout that is self-consistent
 * and different from everyone else's -- a swapped header field, an off-by-one in the offset section,
 * an escape read as a code. Only bytes from a foreign writer catch that, which is what these
 * fixtures are.
 *
 * <p>They come from the C++ implementation's Parquet interop file, and their provenance, layout and
 * per-case coverage are documented in the header of {@code src/test/resources/fsst/interop/
 * expected.txt}. The expected values there were taken from that file's conventionally-encoded
 * columns, which hold the same values as the encoded ones, so nothing in the fixtures depends on any
 * FSST decoder being right.
 */
public class SymbolTableInteropTest {

  private static final String DIRECTORY = "/fsst/interop/";

  /** One case's symbol table and the bodies of its data pages, as the fixture files hold them. */
  private static final class Chunk {
    final SymbolTableType type;
    final byte[] table;
    final List<byte[]> pages;

    Chunk(SymbolTableType type, byte[] table, List<byte[]> pages) {
      this.type = type;
      this.table = table;
      this.pages = pages;
    }

    SymbolTableValuesReader reader() {
      SymbolTable symbolTable = SymbolTables.deserialize(type, table, 0, table.length);
      return new SymbolTableValuesReader(() -> symbolTable);
    }
  }

  /** What one line of {@code expected.txt} asserts: how many values, and which ones. */
  private static final class Expectation {
    final int pageCount;
    final int count;
    final String digest;

    Expectation(int pageCount, int count, String digest) {
      this.pageCount = pageCount;
      this.count = count;
      this.digest = digest;
    }
  }

  private static final List<String> CASES =
      List.of("urls", "nulls-and-empties", "escapes", "escapes-binary", "plain-offsets");

  @Test
  public void everyCaseDecodesToWhatTheReferenceColumnsHold() throws IOException {
    Map<String, Expectation> expected = readExpectations();
    for (String name : CASES) {
      Chunk chunk = readChunk(name);
      Expectation whole = expectation(expected, name);
      assertThat(chunk.pages.size()).as(name + ": page count").isEqualTo(whole.pageCount);

      SymbolTableValuesReader reader = chunk.reader();
      List<byte[]> all = new ArrayList<>();
      for (int page = 0; page < chunk.pages.size(); page++) {
        String pageName = name + "." + page;
        Expectation pageExpectation = expectation(expected, pageName);
        List<byte[]> values = decodePage(reader, chunk.pages.get(page), pageExpectation.count);
        assertThat(digest(values)).as(pageName).isEqualTo(pageExpectation.digest);
        all.addAll(values);
      }
      assertThat(all.size()).as(name + ": value count").isEqualTo(whole.count);
      assertThat(digest(all)).as(name).isEqualTo(whole.digest);
    }
  }

  @Test
  public void bothOffsetSectionEncodingsAreCovered() throws IOException {
    // A writer with few values per page has nothing to gain from packing the offsets, so the file
    // contains pages of each kind and a reader has to tolerate both. Read from the page bodies
    // rather than through the reader, so that a reader which ignored the byte could not hide it.
    Map<OffsetEncoding, Integer> pages = new HashMap<>();
    for (String name : CASES) {
      for (byte[] page : readChunk(name).pages) {
        OffsetEncoding encoding = page[0] == 0 ? OffsetEncoding.PLAIN : OffsetEncoding.DELTA_BINARY_PACKED;
        assertThat(page[0]).as(name + ": offset encoding byte").isIn((byte) 0, (byte) 1);
        pages.merge(encoding, 1, Integer::sum);
      }
    }
    assertThat((int) pages.get(OffsetEncoding.PLAIN))
        .as("pages with a plain offset array")
        .isEqualTo(4);
    assertThat((int) pages.get(OffsetEncoding.DELTA_BINARY_PACKED))
        .as("pages with a packed offset array")
        .isEqualTo(26);
  }

  @Test
  public void theReaderLearnsTheRepresentationFromTheTable() throws IOException {
    Chunk chunk = readChunk("urls");
    SymbolTableValuesReader reader = chunk.reader();
    reader.initFromPage(30, stream(chunk.pages.get(0)));
    assertThat(reader.symbolTableType()).isEqualTo(SymbolTableType.FSST_8);
  }

  @Test
  public void skippingLandsOnTheSameValueAsReadingWould() throws IOException {
    Chunk chunk = readChunk("escapes");
    byte[] page = chunk.pages.get(0);
    int count = expectation(readExpectations(), "escapes.0").count;
    List<byte[]> straight = decodePage(chunk.reader(), page, count);

    for (int skipped = 0; skipped <= count; skipped++) {
      SymbolTableValuesReader reader = chunk.reader();
      reader.initFromPage(count, stream(page));
      reader.skip(skipped);
      for (int i = skipped; i < count; i++) {
        assertThat(reader.readBytes())
            .as("skipped " + skipped + ", value " + i)
            .isEqualTo(Binary.fromConstantByteArray(straight.get(i)));
      }
    }
  }

  private static List<byte[]> decodePage(SymbolTableValuesReader reader, byte[] page, int count) throws IOException {
    reader.initFromPage(count, stream(page));
    List<byte[]> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      values.add(reader.readBytes().getBytes());
    }
    return values;
  }

  private static ByteBufferInputStream stream(byte[] page) {
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(page));
  }

  private static String digest(List<byte[]> values) {
    MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    for (byte[] value : values) {
      sha256.update(new byte[] {
        (byte) (value.length >>> 24),
        (byte) (value.length >>> 16),
        (byte) (value.length >>> 8),
        (byte) value.length
      });
      sha256.update(value);
    }
    StringBuilder hex = new StringBuilder();
    for (byte b : sha256.digest()) {
      hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    }
    return hex.toString();
  }

  private static Chunk readChunk(String name) throws IOException {
    byte[] bytes = resource(name + ".pages");
    int pos = 0;
    SymbolTableType type = SymbolTableType.fromTypeValue(bytes[pos++]);
    int tableLength = readInt(bytes, pos);
    pos += 4;
    byte[] table = new byte[tableLength];
    System.arraycopy(bytes, pos, table, 0, tableLength);
    pos += tableLength;
    int pageCount = readInt(bytes, pos);
    pos += 4;
    List<byte[]> pages = new ArrayList<>(pageCount);
    for (int i = 0; i < pageCount; i++) {
      int length = readInt(bytes, pos);
      pos += 4;
      byte[] page = new byte[length];
      System.arraycopy(bytes, pos, page, 0, length);
      pos += length;
      pages.add(page);
    }
    assertThat(pos).as(name + ": bytes left over in the fixture").isEqualTo(bytes.length);
    return new Chunk(type, table, pages);
  }

  private static int readInt(byte[] bytes, int pos) {
    return ((bytes[pos] & 0xFF) << 24)
        | ((bytes[pos + 1] & 0xFF) << 16)
        | ((bytes[pos + 2] & 0xFF) << 8)
        | (bytes[pos + 3] & 0xFF);
  }

  private static Map<String, Expectation> readExpectations() throws IOException {
    Map<String, Expectation> expectations = new HashMap<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(open("expected.txt"), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.isEmpty() || line.startsWith("#")) {
          continue;
        }
        String[] fields = line.split(" ");
        if (fields.length == 4) {
          // A whole chunk: name, page count, value count, digest.
          expectations.put(
              fields[0],
              new Expectation(Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), fields[3]));
        } else {
          // One page of a chunk: name, value count, digest.
          expectations.put(fields[0], new Expectation(-1, Integer.parseInt(fields[1]), fields[2]));
        }
      }
    }
    return expectations;
  }

  private static Expectation expectation(Map<String, Expectation> expected, String name) {
    Expectation expectation = expected.get(name);
    if (expectation == null) {
      throw new AssertionError("No expectation for " + name + " in " + DIRECTORY + "expected.txt");
    }
    return expectation;
  }

  private static byte[] resource(String name) throws IOException {
    try (InputStream in = open(name)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) > 0) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }

  private static InputStream open(String name) {
    InputStream in = SymbolTableInteropTest.class.getResourceAsStream(DIRECTORY + name);
    if (in == null) {
      throw new UncheckedIOException(new IOException("Missing test resource " + DIRECTORY + name));
    }
    return in;
  }
}
