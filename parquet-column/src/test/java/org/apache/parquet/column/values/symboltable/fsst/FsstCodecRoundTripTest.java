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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.parquet.column.values.symboltable.CodeStreamDecoder;
import org.apache.parquet.column.values.symboltable.CodeStreamEncoder;
import org.apache.parquet.column.values.symboltable.SymbolTable;
import org.apache.parquet.column.values.symboltable.SymbolTableType;
import org.apache.parquet.column.values.symboltable.TrainedSymbolTable;
import org.apache.parquet.column.values.symboltable.ValueBuffer;
import org.junit.jupiter.api.Test;

/**
 * Round trips the FSST codec over inputs chosen to reach the places the port could be wrong: the
 * chunk boundary, the escape path, the byte values a signed Java byte gets wrong, and a table read
 * back from its serialized form rather than the one training produced.
 */
public class FsstCodecRoundTripTest {

  /** Trains on the values, compresses each one, expands it again and requires the bytes back. */
  private static Result roundTrip(List<byte[]> values) throws IOException {
    ValueBuffer buffer = new ValueBuffer();
    for (byte[] value : values) {
      buffer.add(value, 0, value.length);
    }
    TrainedSymbolTable trained = new FsstTrainer().train(buffer);
    SymbolTable table = trained.table();
    CodeStreamEncoder encoder = trained.encoder();

    // Decode through a table read back from bytes, not the one training produced, so the
    // serialization and the renumbering are both under test.
    byte[] serialized = table.serialize().toByteArray();
    SymbolTable reread = Fsst8SymbolTable.deserialize(serialized, 0, serialized.length);
    assertThat(reread.type()).isEqualTo(SymbolTableType.FSST_8);
    assertThat(reread.symbolCount()).isEqualTo(table.symbolCount());
    assertThat(reread.serialize().toByteArray()).isEqualTo(serialized);
    CodeStreamDecoder decoder = reread.decoder();

    int codeBytes = 0;
    int rawBytes = 0;
    for (int i = 0; i < values.size(); i++) {
      byte[] value = values.get(i);
      byte[] codes = new byte[encoder.maxCompressedLength(value.length)];
      int codeLength = encoder.compress(buffer.data(), buffer.offset(i), buffer.length(i), codes, 0);
      assertThat(codeLength).as("compressed past the declared bound").isLessThanOrEqualTo(codes.length);

      assertThat(decoder.expandedLength(codes, 0, codeLength))
          .as("expandedLength disagrees with expand on value " + i)
          .isEqualTo(value.length);
      byte[] expanded = new byte[value.length];
      int written = decoder.expand(codes, 0, codeLength, expanded, 0);
      assertThat(written).as("wrong expanded length for value " + i).isEqualTo(value.length);
      assertThat(expanded)
          .as("value " + i + " did not survive the round trip")
          .isEqualTo(value);

      codeBytes += codeLength;
      rawBytes += value.length;
    }
    return new Result(table, rawBytes, codeBytes);
  }

  private static final class Result {
    final SymbolTable table;
    final int rawBytes;
    final int codeBytes;

    Result(SymbolTable table, int rawBytes, int codeBytes) {
      this.table = table;
      this.rawBytes = rawBytes;
      this.codeBytes = codeBytes;
    }
  }

  private static List<byte[]> strings(String... values) {
    List<byte[]> result = new ArrayList<>();
    for (String value : values) {
      result.add(value.getBytes(StandardCharsets.UTF_8));
    }
    return result;
  }

  @Test
  public void compressesRepetitiveTextAndGetsItBack() throws IOException {
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      values.add(("https://www.example.com/products/widget-" + i + "/reviews?page=" + (i % 7))
          .getBytes(StandardCharsets.UTF_8));
    }
    Result result = roundTrip(values);
    assertThat(result.table.symbolCount())
        .as("the table should hold symbols")
        .isPositive();
    // A corpus this repetitive is the case the encoding exists for; if it does not shrink here the
    // trainer is not finding the shared substrings.
    assertThat(result.codeBytes * 2)
        .as("expected well under half the bytes, got " + result.codeBytes + " of " + result.rawBytes)
        .isLessThan(result.rawBytes);
  }

  @Test
  public void handlesEveryByteValue() throws IOException {
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      byte[] value = new byte[16];
      for (int j = 0; j < value.length; j++) {
        value[j] = (byte) ((i + j) & 0xFF);
      }
      values.add(value);
    }
    roundTrip(values);
  }

  @Test
  public void handlesValuesLongerThanOneChunk() throws IOException {
    // 511 bytes is the chunk the compressor works in, so a value has to be driven across it.
    List<byte[]> values = new ArrayList<>();
    for (int length : new int[] {509, 510, 511, 512, 513, 1021, 1022, 1023, 4096}) {
      StringBuilder builder = new StringBuilder();
      while (builder.length() < length) {
        builder.append("the quick brown fox jumps over the lazy dog ");
      }
      values.add(builder.substring(0, length).getBytes(StandardCharsets.UTF_8));
    }
    roundTrip(values);
  }

  @Test
  public void handlesIncompressibleBytes() throws IOException {
    // Random bytes give the trainer nothing to work with, so nearly every byte escapes.
    Random random = new Random(20260908);
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      byte[] value = new byte[1 + random.nextInt(64)];
      random.nextBytes(value);
      values.add(value);
    }
    Result result = roundTrip(values);
    assertThat(result.codeBytes)
        .as("escaping should cost bytes, not save them")
        .isGreaterThanOrEqualTo(result.rawBytes);
  }

  @Test
  public void handlesEmptyAndTinyInputs() throws IOException {
    roundTrip(new ArrayList<>());
    roundTrip(strings(""));
    roundTrip(strings("a"));
    roundTrip(strings("", "", ""));
    roundTrip(strings("a", "", "bb", "", "ccc"));
  }

  @Test
  public void handlesAValueMadeOnlyOfOneRepeatedByte() throws IOException {
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      byte[] value = new byte[300];
      java.util.Arrays.fill(value, (byte) 0);
      values.add(value);
    }
    roundTrip(values);
  }

  @Test
  public void trainsOnACorpusLargerThanTheSample() throws IOException {
    // Past 16 KiB the trainer samples rather than reading everything, which is a different path.
    Random random = new Random(4637947);
    String[] words = {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"};
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < 8; j++) {
        builder.append(words[random.nextInt(words.length)]).append(' ');
      }
      values.add(builder.toString().getBytes(StandardCharsets.UTF_8));
    }
    Result result = roundTrip(values);
    assertThat(result.codeBytes * 2)
        .as("expected well under half the bytes, got " + result.codeBytes + " of " + result.rawBytes)
        .isLessThan(result.rawBytes);
  }

  @Test
  public void symbolTableStaysWithinItsFormatLimits() throws IOException {
    Random random = new Random(1);
    List<byte[]> values = new ArrayList<>();
    for (int i = 0; i < 3000; i++) {
      byte[] value = new byte[32];
      for (int j = 0; j < value.length; j++) {
        value[j] = (byte) ('a' + random.nextInt(26));
      }
      values.add(value);
    }
    Result result = roundTrip(values);
    assertThat(result.table.symbolCount()).isLessThanOrEqualTo(FsstCodes.MAX_SYMBOLS);
    for (int code = 0; code < result.table.symbolCount(); code++) {
      int length = result.table.symbolLength(code);
      assertThat(length).as("symbol " + code).isBetween(1, 8);
    }
    // The serialized table must be in length order, which is what lets a reader rebuild it from the
    // length histogram alone.
    for (int code = 1; code < result.table.symbolCount(); code++) {
      assertThat(result.table.symbolLength(code - 1))
          .as("symbols are not in length order at code " + code)
          .isLessThanOrEqualTo(result.table.symbolLength(code));
    }
    int size = (int) result.table.serialize().size();
    assertThat(size).as("serialized table size").isBetween(9, 2049);
  }
}
