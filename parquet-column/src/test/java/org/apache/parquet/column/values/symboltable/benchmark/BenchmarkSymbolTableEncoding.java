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
package org.apache.parquet.column.values.symboltable.benchmark;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.annotation.AxisRange;
import com.carrotsearch.junitbenchmarks.annotation.BenchmarkMethodChart;
import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary;
import org.apache.parquet.column.values.symboltable.SymbolTable;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.column.values.symboltable.SymbolTableType;
import org.apache.parquet.column.values.symboltable.SymbolTableValuesReader;
import org.apache.parquet.column.values.symboltable.SymbolTableValuesWriter;
import org.apache.parquet.column.values.symboltable.SymbolTables;
import org.apache.parquet.io.api.Binary;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;

/**
 * Compares FSST against the encodings a text column falls back to today, with and without a zstd
 * second pass, since the layered configuration is the one a real column ever ships with.
 *
 * <p>Every {@code *WithoutZstd}/{@code *WithZstd} pair isolates encode from decode: the bytes a
 * decode benchmark reads are built once outside the timed rounds, and the side channel each
 * encoding needs (a symbol table or a dictionary page) is decoded once as well, matching how a real
 * reader amortizes it across every page of a chunk.
 *
 * <p>Ratios are not a JUnitBenchmarks metric, so they are printed once, up front, rather than folded
 * into a timed round.
 */
@EnableRuleMigrationSupport
@AxisRange(min = 0, max = 1)
@BenchmarkMethodChart(filePrefix = "benchmark-symboltable-encoding")
public class BenchmarkSymbolTableEncoding {

  @Rule
  public org.junit.rules.TestRule benchmarkRun = new BenchmarkRule();

  private static final int INITIAL_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;

  private static final Binary[] VALUES = buildCorpus();
  private static final long RAW_BYTES = totalRawBytes(VALUES);

  // FSST
  private static final byte[] FSST_TABLE_BYTES;
  private static final byte[] FSST_PAYLOAD_BYTES;
  private static final byte[] FSST_PAYLOAD_ZSTD;
  private static final SymbolTable FSST_TABLE;

  // DELTA_LENGTH_BYTE_ARRAY
  private static final byte[] DLBA_PAYLOAD_BYTES;
  private static final byte[] DLBA_PAYLOAD_ZSTD;

  // DELTA_BYTE_ARRAY
  private static final byte[] DBA_PAYLOAD_BYTES;
  private static final byte[] DBA_PAYLOAD_ZSTD;

  // RLE_DICTIONARY
  private static final byte[] DICT_PAGE_BYTES;
  private static final int DICT_SIZE;
  private static final byte[] DICT_INDEX_BYTES;
  private static final byte[] DICT_INDEX_ZSTD;
  private static final DictionaryPage DICTIONARY_PAGE;

  static {
    try {
      SymbolTableValuesWriter fsstWriter = new SymbolTableValuesWriter(
          SymbolTableType.FSST_8,
          OffsetEncoding.DELTA_BINARY_PACKED,
          INITIAL_SLAB_SIZE,
          PAGE_SIZE,
          new DirectByteBufferAllocator());
      writeAll(fsstWriter, VALUES);
      FSST_PAYLOAD_BYTES = fsstWriter.getBytes().toByteArray();
      FSST_TABLE_BYTES = fsstWriter.toSymbolTablePageAndClose().getBytes().toByteArray();
      FSST_TABLE = SymbolTables.deserialize(SymbolTableType.FSST_8, FSST_TABLE_BYTES, 0, FSST_TABLE_BYTES.length);
      FSST_PAYLOAD_ZSTD = Zstd.compress(FSST_PAYLOAD_BYTES);

      DeltaLengthByteArrayValuesWriter dlbaWriter =
          new DeltaLengthByteArrayValuesWriter(INITIAL_SLAB_SIZE, PAGE_SIZE, new DirectByteBufferAllocator());
      writeAll(dlbaWriter, VALUES);
      DLBA_PAYLOAD_BYTES = dlbaWriter.getBytes().toByteArray();
      DLBA_PAYLOAD_ZSTD = Zstd.compress(DLBA_PAYLOAD_BYTES);

      DeltaByteArrayWriter dbaWriter =
          new DeltaByteArrayWriter(INITIAL_SLAB_SIZE, PAGE_SIZE, new DirectByteBufferAllocator());
      writeAll(dbaWriter, VALUES);
      DBA_PAYLOAD_BYTES = dbaWriter.getBytes().toByteArray();
      DBA_PAYLOAD_ZSTD = Zstd.compress(DBA_PAYLOAD_BYTES);

      PlainBinaryDictionaryValuesWriter dictWriter = new PlainBinaryDictionaryValuesWriter(
          Integer.MAX_VALUE, Encoding.RLE_DICTIONARY, Encoding.PLAIN, new DirectByteBufferAllocator());
      writeAll(dictWriter, VALUES);
      DICT_INDEX_BYTES = dictWriter.getBytes().toByteArray();
      DictionaryPage dictPage = dictWriter.toDictPageAndClose();
      DICT_PAGE_BYTES = dictPage.getBytes().toByteArray();
      DICT_SIZE = dictPage.getDictionarySize();
      DICTIONARY_PAGE = new DictionaryPage(BytesInput.from(DICT_PAGE_BYTES), DICT_SIZE, Encoding.PLAIN);
      DICT_INDEX_ZSTD = Zstd.compress(DICT_INDEX_BYTES);

      report();
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /**
   * Text with the shared structure real string columns have: repeated URL templates and repeated
   * vocabulary, the case FSST and dictionary encoding both exist for. Random alphanumeric data, used
   * elsewhere in this module, would flatter nothing and make the comparison meaningless.
   */
  private static Binary[] buildCorpus() {
    String[] words = {
      "alpha",
      "bravo",
      "charlie",
      "delta",
      "echo",
      "foxtrot",
      "golf",
      "hotel",
      "india",
      "juliet",
      "kilo",
      "lima",
      "mike",
      "november",
      "oscar",
      "papa"
    };
    Random random = new Random(42);
    List<String> values = new ArrayList<>();
    for (int i = 0; i < 15000; i++) {
      values.add("https://www.example.com/products/widget-" + i + "/reviews?page=" + (i % 37) + "&sort="
          + (i % 5 == 0 ? "asc" : "desc"));
    }
    for (int i = 0; i < 15000; i++) {
      StringBuilder builder = new StringBuilder();
      int wordCount = 6 + random.nextInt(6);
      for (int j = 0; j < wordCount; j++) {
        if (j > 0) {
          builder.append(' ');
        }
        builder.append(words[random.nextInt(words.length)]);
      }
      builder.append(" order-id=").append(i);
      values.add(builder.toString());
    }
    Collections.shuffle(values, random);
    Binary[] binaries = new Binary[values.size()];
    for (int i = 0; i < binaries.length; i++) {
      binaries[i] = Binary.fromString(values.get(i));
    }
    return binaries;
  }

  private static long totalRawBytes(Binary[] values) {
    long total = 0;
    for (Binary value : values) {
      total += value.length();
    }
    return total;
  }

  private static void writeAll(ValuesWriter writer, Binary[] values) {
    for (Binary value : values) {
      writer.writeBytes(value);
    }
  }

  private static void readAll(ValuesReader reader, ByteBufferInputStream stream, int count) throws IOException {
    reader.initFromPage(count, stream);
    for (int i = 0; i < count; i++) {
      reader.readBytes();
    }
  }

  private static void report() {
    System.out.printf(
        "%-20s %12s %14s %14s %8s %8s%n", "encoding", "raw", "encoded", "encoded+zstd", "ratio", "ratio+zstd");
    reportOne(
        "FSST",
        FSST_TABLE_BYTES.length + FSST_PAYLOAD_BYTES.length,
        FSST_TABLE_BYTES.length + FSST_PAYLOAD_ZSTD.length);
    reportOne("DELTA_LENGTH_BYTE_ARRAY", DLBA_PAYLOAD_BYTES.length, DLBA_PAYLOAD_ZSTD.length);
    reportOne("DELTA_BYTE_ARRAY", DBA_PAYLOAD_BYTES.length, DBA_PAYLOAD_ZSTD.length);
    reportOne(
        "RLE_DICTIONARY",
        DICT_PAGE_BYTES.length + DICT_INDEX_BYTES.length,
        DICT_PAGE_BYTES.length + DICT_INDEX_ZSTD.length);
  }

  private static void reportOne(String name, long encoded, long encodedZstd) {
    System.out.printf(
        "%-20s %12d %14d %14d %8.3f %8.3f%n",
        name, RAW_BYTES, encoded, encodedZstd, (double) encoded / RAW_BYTES, (double) encodedZstd / RAW_BYTES);
  }

  // FSST

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void fsstEncodeWithoutZstd() {
    SymbolTableValuesWriter writer = new SymbolTableValuesWriter(
        SymbolTableType.FSST_8,
        OffsetEncoding.DELTA_BINARY_PACKED,
        INITIAL_SLAB_SIZE,
        PAGE_SIZE,
        new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    writer.getBytes();
    writer.toSymbolTablePageAndClose();
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void fsstEncodeWithZstd() throws IOException {
    SymbolTableValuesWriter writer = new SymbolTableValuesWriter(
        SymbolTableType.FSST_8,
        OffsetEncoding.DELTA_BINARY_PACKED,
        INITIAL_SLAB_SIZE,
        PAGE_SIZE,
        new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    byte[] payload = writer.getBytes().toByteArray();
    byte[] table = writer.toSymbolTablePageAndClose().getBytes().toByteArray();
    Zstd.compress(payload);
    Zstd.compress(table);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void fsstDecodeWithoutZstd() throws IOException {
    SymbolTableValuesReader reader = new SymbolTableValuesReader(() -> FSST_TABLE);
    readAll(reader, BytesInput.from(FSST_PAYLOAD_BYTES).toInputStream(), VALUES.length);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void fsstDecodeWithZstd() throws IOException {
    byte[] payload = Zstd.decompress(FSST_PAYLOAD_ZSTD, FSST_PAYLOAD_BYTES.length);
    SymbolTableValuesReader reader = new SymbolTableValuesReader(() -> FSST_TABLE);
    readAll(reader, BytesInput.from(payload).toInputStream(), VALUES.length);
  }

  // DELTA_LENGTH_BYTE_ARRAY

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaLengthByteArrayEncodeWithoutZstd() {
    DeltaLengthByteArrayValuesWriter writer =
        new DeltaLengthByteArrayValuesWriter(INITIAL_SLAB_SIZE, PAGE_SIZE, new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    writer.getBytes();
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaLengthByteArrayEncodeWithZstd() throws IOException {
    DeltaLengthByteArrayValuesWriter writer =
        new DeltaLengthByteArrayValuesWriter(INITIAL_SLAB_SIZE, PAGE_SIZE, new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    Zstd.compress(writer.getBytes().toByteArray());
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaLengthByteArrayDecodeWithoutZstd() throws IOException {
    readAll(
        new DeltaLengthByteArrayValuesReader(),
        BytesInput.from(DLBA_PAYLOAD_BYTES).toInputStream(),
        VALUES.length);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaLengthByteArrayDecodeWithZstd() throws IOException {
    byte[] payload = Zstd.decompress(DLBA_PAYLOAD_ZSTD, DLBA_PAYLOAD_BYTES.length);
    readAll(new DeltaLengthByteArrayValuesReader(), BytesInput.from(payload).toInputStream(), VALUES.length);
  }

  // DELTA_BYTE_ARRAY

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaByteArrayEncodeWithoutZstd() {
    DeltaByteArrayWriter writer =
        new DeltaByteArrayWriter(INITIAL_SLAB_SIZE, PAGE_SIZE, new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    writer.getBytes();
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaByteArrayEncodeWithZstd() throws IOException {
    DeltaByteArrayWriter writer =
        new DeltaByteArrayWriter(INITIAL_SLAB_SIZE, PAGE_SIZE, new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    Zstd.compress(writer.getBytes().toByteArray());
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaByteArrayDecodeWithoutZstd() throws IOException {
    readAll(new DeltaByteArrayReader(), BytesInput.from(DBA_PAYLOAD_BYTES).toInputStream(), VALUES.length);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void deltaByteArrayDecodeWithZstd() throws IOException {
    byte[] payload = Zstd.decompress(DBA_PAYLOAD_ZSTD, DBA_PAYLOAD_BYTES.length);
    readAll(new DeltaByteArrayReader(), BytesInput.from(payload).toInputStream(), VALUES.length);
  }

  // RLE_DICTIONARY

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void rleDictionaryEncodeWithoutZstd() {
    PlainBinaryDictionaryValuesWriter writer = new PlainBinaryDictionaryValuesWriter(
        Integer.MAX_VALUE, Encoding.RLE_DICTIONARY, Encoding.PLAIN, new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    writer.getBytes();
    writer.toDictPageAndClose();
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void rleDictionaryEncodeWithZstd() throws IOException {
    PlainBinaryDictionaryValuesWriter writer = new PlainBinaryDictionaryValuesWriter(
        Integer.MAX_VALUE, Encoding.RLE_DICTIONARY, Encoding.PLAIN, new DirectByteBufferAllocator());
    writeAll(writer, VALUES);
    byte[] index = writer.getBytes().toByteArray();
    byte[] dict = writer.toDictPageAndClose().getBytes().toByteArray();
    Zstd.compress(index);
    Zstd.compress(dict);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void rleDictionaryDecodeWithoutZstd() throws IOException {
    DictionaryValuesReader reader = new DictionaryValuesReader(new PlainBinaryDictionary(DICTIONARY_PAGE));
    readAll(reader, BytesInput.from(DICT_INDEX_BYTES).toInputStream(), VALUES.length);
  }

  @BenchmarkOptions(benchmarkRounds = 10, warmupRounds = 3)
  @Test
  public void rleDictionaryDecodeWithZstd() throws IOException {
    byte[] index = Zstd.decompress(DICT_INDEX_ZSTD, DICT_INDEX_BYTES.length);
    DictionaryValuesReader reader = new DictionaryValuesReader(new PlainBinaryDictionary(DICTIONARY_PAGE));
    readAll(reader, BytesInput.from(index).toInputStream(), VALUES.length);
  }
}
