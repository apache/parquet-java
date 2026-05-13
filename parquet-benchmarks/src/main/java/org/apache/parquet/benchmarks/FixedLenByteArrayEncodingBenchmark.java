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
package org.apache.parquet.benchmarks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForFLBA;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Encoding-level micro-benchmarks for FIXED_LEN_BYTE_ARRAY (FLBA) values across
 * all supported encodings: PLAIN, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, and DICTIONARY.
 *
 * <p>Each benchmark invocation processes {@value #VALUE_COUNT} values; throughput is
 * reported per-value via {@link OperationsPerInvocation}.
 *
 * <p>The {@code fixedLength} parameter exercises key FLBA sizes:
 * <ul>
 *   <li>2 = FLOAT16</li>
 *   <li>12 = INT96 (legacy timestamps)</li>
 *   <li>16 = UUID</li>
 * </ul>
 *
 * <p>The {@code dataPattern} parameter controls cardinality:
 * <ul>
 *   <li>RANDOM = all unique values</li>
 *   <li>LOW_CARDINALITY = 100 distinct values (favors dictionary and delta)</li>
 * </ul>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class FixedLenByteArrayEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;
  private static final int MAX_DICT_BYTE_SIZE = 4 * 1024 * 1024;

  @Param({"2", "12", "16"})
  public int fixedLength;

  @Param({"RANDOM", "LOW_CARDINALITY"})
  public String dataPattern;

  private Binary[] data;

  // Pre-encoded pages for decode benchmarks
  private byte[] plainEncoded;
  private byte[] deltaEncoded;
  private byte[] bssEncoded;
  private byte[] dictDataEncoded;
  private Dictionary flbaDictionary;
  private boolean dictAvailable;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    int distinct = "LOW_CARDINALITY".equals(dataPattern) ? TestDataFactory.LOW_CARDINALITY_DISTINCT : 0;
    data = TestDataFactory.generateFixedLenByteArrays(
        VALUE_COUNT, fixedLength, distinct, TestDataFactory.DEFAULT_SEED);

    // Pre-encode for decode benchmarks
    plainEncoded = encodeWith(newPlainWriter());
    deltaEncoded = encodeWith(newDeltaWriter());
    bssEncoded = encodeWith(newBssWriter());
    setupDict();
  }

  private byte[] encodeWith(ValuesWriter writer) throws IOException {
    for (Binary v : data) {
      writer.writeBytes(v);
    }
    byte[] bytes = writer.getBytes().toByteArray();
    writer.close();
    return bytes;
  }

  private void setupDict() throws IOException {
    DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE,
            fixedLength,
            Encoding.PLAIN_DICTIONARY,
            Encoding.PLAIN,
            new HeapByteBufferAllocator());
    for (Binary v : data) {
      w.writeBytes(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    dictDataEncoded = enc.dictData;
    dictAvailable = !enc.fellBackToPlain();
    if (dictAvailable) {
      flbaDictionary = new PlainValuesDictionary.PlainBinaryDictionary(enc.dictPage, fixedLength);
    }
  }

  // ---- Writer factories ----

  private FixedLenByteArrayPlainValuesWriter newPlainWriter() {
    return new FixedLenByteArrayPlainValuesWriter(
        fixedLength, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private DeltaByteArrayWriter newDeltaWriter() {
    return new DeltaByteArrayWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter newBssWriter() {
    return new ByteStreamSplitValuesWriter.FixedLenByteArrayByteStreamSplitValuesWriter(
        fixedLength, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  // ==== ENCODE BENCHMARKS ====

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodePlain() throws IOException {
    return encodeWith(newPlainWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodePlainBatch() throws IOException {
    FixedLenByteArrayPlainValuesWriter writer = newPlainWriter();
    writer.writeBinaries(data, 0, data.length);
    byte[] bytes = writer.getBytes().toByteArray();
    writer.close();
    return bytes;
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDelta() throws IOException {
    return encodeWith(newDeltaWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeBss() throws IOException {
    return encodeWith(newBssWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void encodeDictionary(Blackhole bh) throws IOException {
    DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter w =
        new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
            MAX_DICT_BYTE_SIZE,
            fixedLength,
            Encoding.PLAIN_DICTIONARY,
            Encoding.PLAIN,
            new HeapByteBufferAllocator());
    for (Binary v : data) {
      w.writeBytes(v);
    }
    BenchmarkEncodingUtils.EncodedDictionary enc = BenchmarkEncodingUtils.drainDictionary(w);
    bh.consume(enc.dictData);
    bh.consume(enc.dictPage);
  }

  // ==== DECODE BENCHMARKS ====

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodePlain(Blackhole bh) throws IOException {
    FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(fixedLength);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodePlainBatch(Blackhole bh) throws IOException {
    FixedLenByteArrayPlainValuesReader reader = new FixedLenByteArrayPlainValuesReader(fixedLength);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainEncoded)));
    Binary[] batch = new Binary[VALUE_COUNT];
    reader.readBinaries(batch, 0, VALUE_COUNT);
    bh.consume(batch);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDelta(Blackhole bh) throws IOException {
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(deltaEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBss(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(fixedLength);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(bssEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeBssBatch(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForFLBA reader = new ByteStreamSplitValuesReaderForFLBA(fixedLength);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(bssEncoded)));
    Binary[] batch = new Binary[VALUE_COUNT];
    reader.readBinaries(batch, 0, VALUE_COUNT);
    bh.consume(batch);
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDictionary(Blackhole bh) throws IOException {
    if (!dictAvailable) return;
    DictionaryValuesReader reader = new DictionaryValuesReader(flbaDictionary);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(dictDataEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }
}
