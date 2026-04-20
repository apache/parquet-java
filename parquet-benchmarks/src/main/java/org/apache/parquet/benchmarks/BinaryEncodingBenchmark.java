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
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesReader;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.PlainValuesDictionary;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
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
 * Encoding-level and decoding-level micro-benchmarks for BINARY values.
 * Compares PLAIN, DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY, and DICTIONARY encodings
 * across different string lengths and cardinality patterns.
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
public class BinaryEncodingBenchmark {

  static final int VALUE_COUNT = 100_000;
  private static final int INIT_SLAB_SIZE = 64 * 1024;
  private static final int PAGE_SIZE = 4 * 1024 * 1024;
  private static final int MAX_DICT_BYTE_SIZE = 4 * 1024 * 1024;

  @Param({"10", "100", "1000"})
  public int stringLength;

  /** LOW = 100 distinct values; HIGH = all unique. */
  @Param({"LOW", "HIGH"})
  public String cardinality;

  private Binary[] data;
  private byte[] plainEncoded;
  private byte[] deltaLengthEncoded;
  private byte[] deltaStringsEncoded;
  private byte[] dictEncoded;
  private Dictionary binaryDictionary;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    Random random = new Random(42);
    int distinct = "LOW".equals(cardinality) ? TestDataFactory.LOW_CARDINALITY_DISTINCT : 0;
    data = TestDataFactory.generateBinaryData(VALUE_COUNT, stringLength, distinct, random);

    // Pre-encode data for decode benchmarks
    plainEncoded = encodeBinaryWith(newPlainWriter());
    deltaLengthEncoded = encodeBinaryWith(newDeltaLengthWriter());
    deltaStringsEncoded = encodeBinaryWith(newDeltaStringsWriter());

    DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter dictWriter = newDictWriter();
    for (Binary v : data) {
      dictWriter.writeBytes(v);
    }
    dictEncoded = dictWriter.getBytes().toByteArray();
    DictionaryPage dictPage = dictWriter.toDictPageAndClose().copy();
    binaryDictionary = new PlainValuesDictionary.PlainBinaryDictionary(dictPage);
    dictWriter.close();
  }

  private byte[] encodeBinaryWith(ValuesWriter writer) throws IOException {
    for (Binary v : data) {
      writer.writeBytes(v);
    }
    byte[] bytes = writer.getBytes().toByteArray();
    writer.close();
    return bytes;
  }

  private byte[] encodeDictionaryWith(DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter writer)
      throws IOException {
    for (Binary v : data) {
      writer.writeBytes(v);
    }
    BytesInput dataBytes = writer.getBytes();
    DictionaryPage dictPage = writer.toDictPageAndClose();
    byte[] bytes;
    if (dictPage == null) {
      bytes = dataBytes.toByteArray();
    } else {
      BytesInput allBytes = BytesInput.concat(dataBytes, dictPage.getBytes());
      bytes = allBytes.toByteArray();
    }
    writer.close();
    return bytes;
  }

  // ---- Writer factories ----

  private static PlainValuesWriter newPlainWriter() {
    return new PlainValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DeltaLengthByteArrayValuesWriter newDeltaLengthWriter() {
    return new DeltaLengthByteArrayValuesWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DeltaByteArrayWriter newDeltaStringsWriter() {
    return new DeltaByteArrayWriter(INIT_SLAB_SIZE, PAGE_SIZE, new HeapByteBufferAllocator());
  }

  private static DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter newDictWriter() {
    return new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(
        MAX_DICT_BYTE_SIZE, Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, new HeapByteBufferAllocator());
  }

  // ---- Encode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodePlain() throws IOException {
    return encodeBinaryWith(newPlainWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDeltaLengthByteArray() throws IOException {
    return encodeBinaryWith(newDeltaLengthWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDeltaByteArray() throws IOException {
    return encodeBinaryWith(newDeltaStringsWriter());
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public byte[] encodeDictionary() throws IOException {
    return encodeDictionaryWith(newDictWriter());
  }

  // ---- Decode benchmarks ----

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodePlain(Blackhole bh) throws IOException {
    BinaryPlainValuesReader reader = new BinaryPlainValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(plainEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDeltaLengthByteArray(Blackhole bh) throws IOException {
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(deltaLengthEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDeltaByteArray(Blackhole bh) throws IOException {
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(deltaStringsEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }

  @Benchmark
  @OperationsPerInvocation(VALUE_COUNT)
  public void decodeDictionary(Blackhole bh) throws IOException {
    DictionaryValuesReader reader = new DictionaryValuesReader(binaryDictionary);
    reader.initFromPage(VALUE_COUNT, ByteBufferInputStream.wrap(ByteBuffer.wrap(dictEncoded)));
    for (int i = 0; i < VALUE_COUNT; i++) {
      bh.consume(reader.readBytes());
    }
  }
}
