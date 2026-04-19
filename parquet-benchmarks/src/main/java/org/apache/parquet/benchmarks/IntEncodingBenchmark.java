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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesReaderForInteger;
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.column.values.plain.PlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
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
 * Encoding-level micro-benchmarks for INT32 values.
 * Compares PLAIN, DELTA_BINARY_PACKED, BYTE_STREAM_SPLIT, and DICTIONARY encodings
 * across different data distribution patterns.
 *
 * <p>Each benchmark invocation processes {@value #VALUE_COUNT} values. Throughput is
 * reported per-value using {@link OperationsPerInvocation}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class IntEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 1024 * 1024;
  private static final int MAX_DICT_BYTE_SIZE = 1024 * 1024;

  @Param({"SEQUENTIAL", "RANDOM", "LOW_CARDINALITY", "HIGH_CARDINALITY"})
  public String dataPattern;

  private int[] data;
  private byte[] plainEncoded;
  private byte[] deltaEncoded;
  private byte[] bssEncoded;
  private byte[] rleEncoded;
  private int rleBitWidth;
  private byte[] dictDataEncoded;
  private Dictionary intDictionary;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    switch (dataPattern) {
      case "SEQUENTIAL":
        data = TestDataFactory.generateSequentialInts(VALUE_COUNT);
        break;
      case "RANDOM":
        data = TestDataFactory.generateRandomInts(VALUE_COUNT, random);
        break;
      case "LOW_CARDINALITY":
        data = TestDataFactory.generateLowCardinalityInts(
            VALUE_COUNT, TestDataFactory.LOW_CARDINALITY_DISTINCT, random);
        break;
      case "HIGH_CARDINALITY":
        data = TestDataFactory.generateHighCardinalityInts(VALUE_COUNT);
        break;
      default:
        throw new IllegalArgumentException("Unknown data pattern: " + dataPattern);
    }

    // Pre-encode data for decode benchmarks
    plainEncoded = encodeWith(newPlainWriter());
    deltaEncoded = encodeWith(newDeltaWriter());
    bssEncoded = encodeWith(newBssWriter());

    // Pre-encode RLE data (using 10-bit values to simulate dictionary indices)
    rleBitWidth = 10;
    RunLengthBitPackingHybridEncoder rleEncoder = new RunLengthBitPackingHybridEncoder(
        rleBitWidth, INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
    for (int v : data) {
      rleEncoder.writeInt(v & 0x3FF); // mask to 10 bits
    }
    rleEncoded = rleEncoder.toBytes().toByteArray();
    rleEncoder.close();

    // Pre-encode dictionary data for decode benchmark
    DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter dictWriter = newDictWriter();
    for (int v : data) {
      dictWriter.writeInteger(v);
    }
    BytesInput dictDataBytes = dictWriter.getBytes();
    dictDataEncoded = dictDataBytes.toByteArray();
    DictionaryPage dictPage = dictWriter.toDictPageAndClose().copy();
    intDictionary = new PlainValuesDictionary.PlainIntegerDictionary(dictPage);
  }

  private byte[] encodeWith(ValuesWriter writer) throws IOException {
    for (int v : data) {
      writer.writeInteger(v);
    }
    byte[] bytes = writer.getBytes().toByteArray();
    writer.close();
    return bytes;
  }

  // ---- Writer factories ----

  private static PlainValuesWriter newPlainWriter() {
    return new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DeltaBinaryPackingValuesWriterForInteger newDeltaWriter() {
    return new DeltaBinaryPackingValuesWriterForInteger(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter newBssWriter() {
    return new ByteStreamSplitValuesWriter.IntegerByteStreamSplitValuesWriter(
        INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter newDictWriter() {
    return new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(
        MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
  }

  // ---- Encode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodePlain() throws IOException {
    return encodeWith(newPlainWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDelta() throws IOException {
    return encodeWith(newDeltaWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeByteStreamSplit() throws IOException {
    return encodeWith(newBssWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDictionary() throws IOException {
    return encodeWith(newDictWriter());
  }

  // ---- Decode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodePlain(Blackhole bh) throws IOException {
    PlainValuesReader.IntegerPlainValuesReader reader = new PlainValuesReader.IntegerPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDelta(Blackhole bh) throws IOException {
    DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(deltaEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeByteStreamSplit(Blackhole bh) throws IOException {
    ByteStreamSplitValuesReaderForInteger reader = new ByteStreamSplitValuesReaderForInteger();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(bssEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeRle(Blackhole bh) throws IOException {
    RunLengthBitPackingHybridDecoder decoder = new RunLengthBitPackingHybridDecoder(
        rleBitWidth, ByteBufferInputStream.wrap(ByteBuffer.wrap(rleEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(decoder.readInt());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDictionary(Blackhole bh) throws IOException {
    DictionaryValuesReader reader = new DictionaryValuesReader(intDictionary);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(dictDataEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readInteger());
    }
  }
}
