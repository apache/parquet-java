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
package org.apache.parquet.column.values.symboltable.fsst;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.values.symboltable.CodeStreamEncoder;
import org.apache.parquet.column.values.symboltable.TrainedSymbolTable;
import org.apache.parquet.column.values.symboltable.ValueBuffer;
import org.junit.Test;

/**
 * Requires this implementation to produce the same symbol table and the same code bytes as the
 * reference implementation, for corpora chosen to reach the places a port can drift.
 *
 * <p>This is the test that makes the port trustworthy. A round trip only proves the decoder undoes
 * whatever the encoder did, so a trainer that picks worse symbols, or numbers them differently,
 * passes it while writing files no other implementation would have written. Comparing against bytes
 * the reference produced catches both.
 *
 * <p>The fixtures under {@code src/test/resources/fsst} hold, per corpus, the serialized symbol table
 * and the concatenated code bytes, produced by the reference implementation. The corpora themselves
 * are not stored: they are generated here from the arithmetic below, which is simple enough to
 * restate in any language, and each one's digest is asserted so a drifting generator fails as a
 * generator rather than as a codec.
 */
public class FsstReferenceComparisonTest {

  /**
   * The generator both sides use, spelled out rather than taken from a library so that regenerating
   * the fixtures from another language gives the same bytes.
   */
  private static final class Lcg {
    private int state;

    Lcg(int seed) {
      this.state = seed & 0x7FFFFFFF;
    }

    int next() {
      state = (state * 1103515245 + 12345) & 0x7FFFFFFF;
      return state;
    }

    int below(int bound) {
      return next() % bound;
    }

    int nextByte() {
      return (next() >> 16) & 0xFF;
    }
  }

  private static final String[] WORDS = {
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf",
    "hotel", "india", "juliet", "kilo", "lima", "mike", "november"
  };

  /** Ordinary text with heavy shared structure: what the encoding is for. */
  private static List<byte[]> urls() {
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      values.add(ascii("https://www.example.com/products/widget-" + i + "/reviews?page=" + (i % 7)));
    }
    return values;
  }

  /** Past the sample target, so the trainer samples instead of reading everything. */
  private static List<byte[]> sampled() {
    Lcg random = new Lcg(1);
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 1200; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < 9; j++) {
        if (j > 0) {
          builder.append(' ');
        }
        builder.append(WORDS[random.below(WORDS.length)]);
      }
      values.add(ascii(builder + " id=" + i));
    }
    return values;
  }

  /** Nothing to compress, so nearly every byte escapes and the table saturates. */
  private static List<byte[]> binary() {
    Lcg random = new Lcg(2);
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      // The length is drawn before any of the bytes, which is the order the fixtures were made in.
      int length = 1 + random.below(64);
      byte[] value = new byte[length];
      for (int j = 0; j < length; j++) {
        value[j] = (byte) random.nextByte();
      }
      values.add(value);
    }
    return values;
  }

  /** Every byte value, including the ones a signed Java byte gets wrong. */
  private static List<byte[]> allBytes() {
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      byte[] value = new byte[16];
      for (int j = 0; j < value.length; j++) {
        value[j] = (byte) ((i + j) & 0xFF);
      }
      values.add(value);
    }
    return values;
  }

  /** Values straddling the 511-byte chunk the compressor works in. */
  private static List<byte[]> chunks() {
    String base = "the quick brown fox jumps over the lazy dog ";
    List<byte[]> values = new ArrayList<>();
    for (int length : new int[] {509, 510, 511, 512, 513, 1021, 1022, 1023, 4096}) {
      StringBuilder builder = new StringBuilder();
      while (builder.length() < length) {
        builder.append(base);
      }
      values.add(ascii(builder.substring(0, length)));
    }
    return values;
  }

  private static List<byte[]> empties() {
    List<byte[]> values = new ArrayList<>();
    for (String value : new String[] {"", "a", "", "bb", "", "ccc", ""}) {
      values.add(ascii(value));
    }
    return values;
  }

  private static byte[] ascii(String value) {
    return value.getBytes(StandardCharsets.ISO_8859_1);
  }

  @Test
  public void matchesTheReferenceOnSharedStructure() throws IOException {
    check("urls", urls(), "f3f5706428f9100812a29c36d865faf7298bd84b0bdec2d4090f1a5aee0f2f7d");
  }

  @Test
  public void matchesTheReferenceWhenTheTrainerSamples() throws IOException {
    check("sampled", sampled(), "f1517b97140990c898ce71a9acf144c4fa670afb4e391d4682972710965e6f64");
  }

  @Test
  public void matchesTheReferenceOnIncompressibleBytes() throws IOException {
    check("binary", binary(), "3b1a118902ff50b0359663084ac76190d0e865c6ce3774c5085187ebebdd38a6");
  }

  @Test
  public void matchesTheReferenceOnEveryByteValue() throws IOException {
    check("allbytes", allBytes(), "db4288e84084c52f1dbb79b88715529fa50c4ddd4159307c562c1f93e8521d0d");
  }

  @Test
  public void matchesTheReferenceAcrossTheChunkBoundary() throws IOException {
    check("chunks", chunks(), "9163babc5f91c5bd2564a64ee6f40f0968595fc93e66e89db8001e8636f9e083");
  }

  @Test
  public void matchesTheReferenceOnEmptyAndTinyValues() throws IOException {
    check("empties", empties(), "807d87de83260feea2276cabc85fe028f9c35d439a05d6eb50689e4873c8945b");
  }

  private void check(String corpus, List<byte[]> values, String expectedDigest) throws IOException {
    ValueBuffer buffer = new ValueBuffer();
    for (byte[] value : values) {
      buffer.add(value, 0, value.length);
    }
    assertEquals(
        corpus + ": the generated corpus does not match the one the fixtures were made from",
        expectedDigest,
        sha256(buffer.data(), buffer.byteCount()));

    TrainedSymbolTable trained = new FsstTrainer().train(buffer);
    assertArrayEquals(
        corpus + ": symbol table differs from the reference implementation",
        resource(corpus + ".table"),
        trained.table().serialize().toByteArray());

    CodeStreamEncoder encoder = trained.encoder();
    ByteArrayOutputStream codes = new ByteArrayOutputStream();
    for (int i = 0; i < values.size(); i++) {
      byte[] output = new byte[encoder.maxCompressedLength(buffer.length(i))];
      int length = encoder.compress(buffer.data(), buffer.offset(i), buffer.length(i), output, 0);
      codes.write(output, 0, length);
    }
    assertArrayEquals(
        corpus + ": code bytes differ from the reference implementation",
        resource(corpus + ".codes"),
        codes.toByteArray());
  }

  private static byte[] resource(String name) throws IOException {
    try (InputStream in = FsstReferenceComparisonTest.class.getResourceAsStream("/fsst/" + name)) {
      if (in == null) {
        throw new IOException("missing test resource /fsst/" + name);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] chunk = new byte[8192];
      int read;
      while ((read = in.read(chunk)) > 0) {
        out.write(chunk, 0, read);
      }
      return out.toByteArray();
    }
  }

  private static String sha256(byte[] data, int length) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(data, 0, length);
      StringBuilder hex = new StringBuilder();
      for (byte b : digest.digest()) {
        hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      return hex.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }
}
