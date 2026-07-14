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

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Isolated JMH benchmarks for raw Parquet compression and decompression throughput.
 *
 * <p>Measures the performance of {@link CompressionCodecFactory.BytesInputCompressor}
 * and {@link CompressionCodecFactory.BytesInputDecompressor} for each supported codec,
 * comparing the heap-based {@link CodecFactory} path (what all production users take)
 * against the direct-memory {@code DirectCodecFactory} path (off-heap ByteBuffers).
 *
 * <p>This benchmark isolates the codec hot path from file I/O and other Parquet overhead,
 * while still feeding the compressor <em>exactly the bytes production does</em>: each
 * {@code dataShape} is a real Parquet <b>data page</b>, produced by running realistic typed
 * column values through Parquet's actual {@link ValuesWriter} encoders (PLAIN,
 * dictionary+RLE, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, RLE booleans).
 * This matters because compressors never see raw values — they see encoded pages, whose byte
 * distribution is entirely determined by the encoding. Hand-rolled byte patterns (e.g. 4-byte
 * little-endian "dictionary indices" or uniform-random doubles) mis-represent that and skew
 * codec-specific results.
 *
 * <p><b>Shape selection — by byte distribution, not by combinatorial matrix.</b> A block
 * compressor only ever sees the <em>encoded page bytes</em>, so its behaviour is a function of the
 * byte distribution alone. Shapes are therefore chosen to span the distinct byte layouts that real
 * Parquet pages exhibit — the {@code (physical type, encoding, value distribution)} combinations
 * that produce <em>materially different</em> bytes — rather than an exhaustive type×encoding matrix.
 * Two combinations that emit the same layout compress identically, so only one is kept. Every
 * non-deprecated data-page encoding is represented at least once.
 *
 * <p><b>Data shapes</b> (type / encoding / semantics):
 *
 * <ul>
 *   <li>{@code INT32_ID_PLAIN} — random 32-bit ids, PLAIN (V1-written files).</li>
 *   <li>{@code INT32_DELTA} — ascending 32-bit values, DELTA_BINARY_PACKED.</li>
 *   <li>{@code INT32_ENUM_DICT} — low-cardinality category ids, dictionary + RLE indices.</li>
 *   <li>{@code INT64_TIMESTAMP_DELTA} — event timestamps, DELTA_BINARY_PACKED.</li>
 *   <li>{@code FLOAT_MEASURE_PLAIN} — clustered 32-bit readings, PLAIN.</li>
 *   <li>{@code FLOAT_MEASURE_BSS} — the same 32-bit readings, BYTE_STREAM_SPLIT.</li>
 *   <li>{@code DOUBLE_MEASURE_PLAIN} — clustered 64-bit readings, PLAIN.</li>
 *   <li>{@code DOUBLE_MEASURE_BSS} — the same 64-bit readings, BYTE_STREAM_SPLIT.</li>
 *   <li>{@code FLBA_DECIMAL_PLAIN} — DECIMAL as 16-byte fixed-length, PLAIN.</li>
 *   <li>{@code STRING_CATEGORY_DICT} — categorical strings, dictionary + RLE indices (data page).</li>
 *   <li>{@code STRING_DICT_PAGE_PLAIN} — distinct strings, PLAIN — the layout of a dictionary
 *       <em>page</em> (present in both V1 and V2 files).</li>
 *   <li>{@code STRING_TEXT_PLAIN} — free text with repetition, PLAIN (V1 data page).</li>
 *   <li>{@code STRING_TEXT_DELTA} — free text, DELTA_BYTE_ARRAY.</li>
 *   <li>{@code STRING_DELTA_LENGTH} — free text, DELTA_LENGTH_BYTE_ARRAY.</li>
 *   <li>{@code BOOLEAN_FLAG_RLE} — mostly-true flag with runs, RLE.</li>
 *   <li>{@code BINARY_UUID_RANDOM} — random 16-byte ids (hashes/uuids); compression floor.</li>
 * </ul>
 *
 * <p><b>Deliberately not tested (byte-distribution-redundant, or deprecated).</b> The following
 * would not add a distinct byte layout, so testing them would only inflate the matrix:
 *
 * <ul>
 *   <li><b>Dictionary data pages for other types</b> (INT64/FLOAT/DOUBLE/FLBA · RLE_DICTIONARY):
 *       a dictionary data page is RLE/bit-packed integer indices whose layout depends on the
 *       dictionary <em>cardinality</em>, not the value type — byte-identical to
 *       {@code INT32_ENUM_DICT} / {@code STRING_CATEGORY_DICT}.</li>
 *   <li><b>PLAIN_DICTIONARY</b> (legacy V1 dictionary): its data page is identical to
 *       RLE_DICTIONARY; only the encoding id and the dictionary page differ (the latter's PLAIN
 *       layout is covered by {@code STRING_DICT_PAGE_PLAIN}).</li>
 *   <li><b>FIXED_LEN_BYTE_ARRAY · DELTA_BYTE_ARRAY</b> (the V2 default for FLBA): same encoding
 *       and layout as {@code STRING_TEXT_DELTA}.</li>
 *   <li><b>INT64 · PLAIN</b>: the same little-endian-integer layout as {@code INT32_ID_PLAIN},
 *       only wider (extra high-order zero bytes) — no new distribution.</li>
 *   <li><b>BIT_PACKED</b>: used for definition/repetition levels only, and deprecated.</li>
 *   <li><b>INT96 · PLAIN</b> (deprecated legacy timestamps) and <b>BYTE_STREAM_SPLIT for
 *       INT32/INT64/FLBA</b> (spec-extended but rare): distinct but low-value today; add them only
 *       when specifically targeting legacy lakes or BSS-on-integer workloads.</li>
 * </ul>
 *
 * <p><b>Methodology.</b> Results are highly data-dependent, so vary {@code dataShape}. Compare
 * codecs/paths only within the same {@code (codec, pageSize, dataShape)} cell. {@code pageSize}
 * is the target for the volume of buffered values; the resulting encoded page (what is actually
 * compressed) may be smaller, especially for dictionary/RLE shapes. JMH runs each {@code @Param}
 * combination in its own forked JVM ({@code @Fork}); do <em>not</em> substitute ad-hoc single-JVM
 * timing loops that iterate over codecs/shapes, which cross-contaminate JIT profiles and produce
 * misleading deltas. The full parameter matrix is large; run targeted subsets via the JMH CLI,
 * e.g. {@code -p codec=ZSTD -p dataShape=DOUBLE_MEASURE_PLAIN -p pageSize=1048576}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Thread)
public class CompressionBenchmark {

  @Param({"SNAPPY", "ZSTD", "LZ4_RAW", "LZ4", "GZIP", "BROTLI", "LZO"})
  public String codec;

  @Param({"65536", "131072", "262144", "1048576"})
  public int pageSize;

  @Param({"HEAP", "DIRECT"})
  public String factoryType;

  @Param({
    "INT32_ID_PLAIN",
    "INT32_DELTA",
    "INT32_ENUM_DICT",
    "INT64_TIMESTAMP_DELTA",
    "FLOAT_MEASURE_PLAIN",
    "FLOAT_MEASURE_BSS",
    "DOUBLE_MEASURE_PLAIN",
    "DOUBLE_MEASURE_BSS",
    "FLBA_DECIMAL_PLAIN",
    "STRING_CATEGORY_DICT",
    "STRING_DICT_PAGE_PLAIN",
    "STRING_TEXT_PLAIN",
    "STRING_TEXT_DELTA",
    "STRING_DELTA_LENGTH",
    "BOOLEAN_FLAG_RLE",
    "BINARY_UUID_RANDOM"
  })
  public String dataShape;

  private byte[] uncompressedData;
  private byte[] compressedData;
  private int decompressedSize;

  private CompressionCodecFactory.BytesInputCompressor compressor;
  private CompressionCodecFactory.BytesInputDecompressor decompressor;
  private CodecFactory factory;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    uncompressedData = buildEncodedPage(dataShape, pageSize, 42L);
    decompressedSize = uncompressedData.length;

    Configuration conf = new Configuration();
    if ("DIRECT".equals(factoryType)) {
      factory = CodecFactory.createDirectCodecFactory(conf, DirectByteBufferAllocator.getInstance(), pageSize);
    } else {
      factory = new CodecFactory(conf, pageSize);
    }
    CompressionCodecName codecName = CompressionCodecName.valueOf(codec);

    compressor = factory.getCompressor(codecName);
    decompressor = factory.getDecompressor(codecName);

    // Pre-compress for decompression benchmark; copy to a stable byte array
    // since the compressor may reuse its internal buffer.
    BytesInput compressed = compressor.compress(BytesInput.from(uncompressedData));
    compressedData = compressed.toByteArray();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    factory.release();
  }

  @Benchmark
  public BytesInput compress() throws IOException {
    return compressor.compress(BytesInput.from(uncompressedData));
  }

  @Benchmark
  public byte[] decompress() throws IOException {
    // Force materialization of the decompressed data. toByteArray() is essentially
    // free for our optimized implementations (returns the existing byte[]).
    return decompressor
        .decompress(BytesInput.from(compressedData), decompressedSize)
        .toByteArray();
  }

  // ---------------------------------------------------------------------------
  // Realistic page generation: real typed values -> real Parquet encoders.
  // ---------------------------------------------------------------------------

  /**
   * Builds a real Parquet data page for the given {@code shape} by writing realistic typed
   * values through Parquet's own {@link ValuesWriter}, then returning the encoded page bytes
   * (exactly what the compressor sees in production). {@code targetSize} bounds the volume of
   * buffered values.
   */
  private byte[] buildEncodedPage(String shape, int targetSize, long seed) throws IOException {
    Random r = new Random(seed);
    switch (shape) {
      case "INT32_ID_PLAIN": {
        // V1 writers emit PLAIN for ints (V2 uses DELTA); both are common in the wild.
        ValuesWriter w = newWriter(PARQUET_1_0, PrimitiveTypeName.INT32, 0, false, false);
        return fill(w, targetSize, () -> w.writeInteger(r.nextInt()));
      }
      case "INT32_DELTA": {
        ValuesWriter w = newWriter(PrimitiveTypeName.INT32, false, false);
        int[] v = {r.nextInt(1000)};
        return fill(w, targetSize, () -> {
          v[0] += r.nextInt(16); // small ascending steps (row numbers / offsets)
          w.writeInteger(v[0]);
        });
      }
      case "INT32_ENUM_DICT": {
        ValuesWriter w = newWriter(PrimitiveTypeName.INT32, true, false);
        int[] cats = distinctInts(r, 40);
        return fill(w, targetSize, () -> w.writeInteger(cats[skewed(r, cats.length)]));
      }
      case "INT64_TIMESTAMP_DELTA": {
        ValuesWriter w = newWriter(PrimitiveTypeName.INT64, false, false);
        long[] ts = {1_600_000_000_000L};
        return fill(w, targetSize, () -> {
          ts[0] += 1000 + r.nextInt(5000); // ~event cadence in millis
          w.writeLong(ts[0]);
        });
      }
      case "FLOAT_MEASURE_PLAIN": {
        ValuesWriter w = newWriter(PrimitiveTypeName.FLOAT, false, false);
        return fill(w, targetSize, () -> w.writeFloat(measurementF(r)));
      }
      case "FLOAT_MEASURE_BSS": {
        ValuesWriter w = newWriter(PrimitiveTypeName.FLOAT, false, true);
        return fill(w, targetSize, () -> w.writeFloat(measurementF(r)));
      }
      case "DOUBLE_MEASURE_PLAIN": {
        ValuesWriter w = newWriter(PrimitiveTypeName.DOUBLE, false, false);
        return fill(w, targetSize, () -> w.writeDouble(measurement(r)));
      }
      case "DOUBLE_MEASURE_BSS": {
        ValuesWriter w = newWriter(PrimitiveTypeName.DOUBLE, false, true);
        return fill(w, targetSize, () -> w.writeDouble(measurement(r)));
      }
      case "FLBA_DECIMAL_PLAIN": {
        // V2 defaults FLBA to DELTA_BYTE_ARRAY; PLAIN fixed-length (classic decimal storage) is V1.
        ValuesWriter w = newWriter(PARQUET_1_0, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 16, false, false);
        return fill(w, targetSize, () -> w.writeBytes(decimal16(r)));
      }
      case "STRING_CATEGORY_DICT": {
        ValuesWriter w = newWriter(PrimitiveTypeName.BINARY, true, false);
        Binary[] cats = distinctStrings(r, 50, 3, 12);
        return fill(w, targetSize, () -> w.writeBytes(cats[skewed(r, cats.length)]));
      }
      case "STRING_DICT_PAGE_PLAIN": {
        // Distinct values in PLAIN layout ([len][bytes], no value repetition) — the byte layout
        // of a dictionary page, which is PLAIN-encoded in both V1 and V2 files.
        ValuesWriter w = newWriter(PARQUET_1_0, PrimitiveTypeName.BINARY, 0, false, false);
        return fill(w, targetSize, () -> w.writeBytes(randomToken(r, 4, 16)));
      }
      case "STRING_TEXT_PLAIN": {
        // PLAIN binary data pages only occur under V1 (V2 binary defaults to DELTA_BYTE_ARRAY).
        ValuesWriter w = newWriter(PARQUET_1_0, PrimitiveTypeName.BINARY, 0, false, false);
        return fill(w, targetSize, () -> w.writeBytes(Binary.fromString(sentence(r))));
      }
      case "STRING_TEXT_DELTA": {
        ValuesWriter w = newWriter(PrimitiveTypeName.BINARY, false, false);
        return fill(w, targetSize, () -> w.writeBytes(Binary.fromString(sentence(r))));
      }
      case "STRING_DELTA_LENGTH": {
        // DELTA_LENGTH_BYTE_ARRAY is not a factory default; construct it directly.
        ValuesWriter w = new DeltaLengthByteArrayValuesWriter(
            64 * 1024, 64 * 1024 * 1024, new HeapByteBufferAllocator());
        return fill(w, targetSize, () -> w.writeBytes(Binary.fromString(sentence(r))));
      }
      case "BOOLEAN_FLAG_RLE": {
        ValuesWriter w = newWriter(PrimitiveTypeName.BOOLEAN, false, false);
        boolean[] state = {true};
        return fill(w, targetSize, () -> {
          if (r.nextInt(100) < 5) {
            state[0] = !state[0]; // ~5% flip probability -> realistic runs
          }
          w.writeBoolean(state[0]);
        });
      }
      case "BINARY_UUID_RANDOM": {
        ValuesWriter w = newWriter(PrimitiveTypeName.BINARY, false, false);
        return fill(w, targetSize, () -> {
          byte[] b = new byte[16];
          r.nextBytes(b);
          w.writeBytes(Binary.fromConstantByteArray(b));
        });
      }
      default:
        throw new IllegalArgumentException("Unknown data shape: " + shape);
    }
  }

  /** Creates the {@link ValuesWriter} Parquet would use for {@code type} under the given options. */
  private ValuesWriter newWriter(PrimitiveTypeName type, boolean dictionary, boolean byteStreamSplit) {
    return newWriter(PARQUET_2_0, type, 0, dictionary, byteStreamSplit);
  }

  private ValuesWriter newWriter(
      WriterVersion version,
      PrimitiveTypeName type,
      int typeLength,
      boolean dictionary,
      boolean byteStreamSplit) {
    ParquetProperties props = ParquetProperties.builder()
        .withWriterVersion(version)
        .withDictionaryEncoding(dictionary)
        .withByteStreamSplitEncoding(byteStreamSplit)
        .withAllocator(new HeapByteBufferAllocator())
        .withPageSize(64 * 1024 * 1024) // large so the writer never self-limits during fill
        .build();
    ColumnDescriptor col = typeLength > 0
        ? new ColumnDescriptor(new String[] {"col"}, type, typeLength, 0, 0)
        : new ColumnDescriptor(new String[] {"col"}, type, 0, 0);
    return props.newValuesWriter(col);
  }

  /** Writes values until the buffered volume reaches {@code targetSize}, then returns the page. */
  private static byte[] fill(ValuesWriter w, int targetSize, Runnable writeOne) throws IOException {
    try {
      long cap = (long) targetSize * 8L + 1024L; // hard bound so we always terminate
      long n = 0;
      while (w.getBufferedSize() < targetSize && n < cap) {
        writeOne.run();
        n++;
      }
      return w.getBytes().toByteArray();
    } finally {
      try {
        w.close();
      } catch (Exception ignored) {
        // best-effort release of writer buffers during benchmark setup
      }
    }
  }

  private static int[] distinctInts(Random r, int count) {
    int[] out = new int[count];
    for (int i = 0; i < count; i++) {
      out[i] = 1000 + r.nextInt(1_000_000);
    }
    return out;
  }

  private static Binary[] distinctStrings(Random r, int count, int minLen, int maxLen) {
    Binary[] out = new Binary[count];
    for (int i = 0; i < count; i++) {
      out[i] = randomToken(r, minLen, maxLen);
    }
    return out;
  }

  /** A single random uppercase token of length in {@code [minLen, maxLen]}. */
  private static Binary randomToken(Random r, int minLen, int maxLen) {
    int len = minLen + r.nextInt(maxLen - minLen + 1);
    StringBuilder sb = new StringBuilder(len);
    for (int j = 0; j < len; j++) {
      sb.append((char) ('A' + r.nextInt(26)));
    }
    return Binary.fromString(sb.toString());
  }

  /** Mild power-law skew towards lower indices, as real categorical distributions exhibit. */
  private static int skewed(Random r, int n) {
    int idx = (int) (n * r.nextDouble() * r.nextDouble());
    return Math.min(idx, n - 1);
  }

  /** Sensor-like reading: Gaussian around 15.0 with 8.0 spread, quantized to 0.1. */
  private static double measurement(Random r) {
    return Math.round((15.0 + r.nextGaussian() * 8.0) * 10.0) / 10.0;
  }

  /** 32-bit sensor-like reading: Gaussian around 15.0 with 8.0 spread, quantized to 0.1. */
  private static float measurementF(Random r) {
    return Math.round((15.0f + (float) r.nextGaussian() * 8.0f) * 10.0f) / 10.0f;
  }

  /**
   * DECIMAL-like value as a 16-byte big-endian FIXED_LEN_BYTE_ARRAY: a bounded unscaled magnitude
   * (e.g. a price in cents) placed in the low bytes, leaving the high bytes zero — the sparse,
   * leading-zero layout real decimal columns exhibit.
   */
  private static Binary decimal16(Random r) {
    long unscaled = (long) (r.nextDouble() * 100_000_000L);
    byte[] b = new byte[16];
    for (int i = 0; i < 8; i++) {
      b[15 - i] = (byte) (unscaled >>> (8 * i));
    }
    return Binary.fromConstantByteArray(b);
  }

  private static final String[] WORDS = {
    "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "parquet", "column",
    "data", "value", "record", "field", "schema", "compression", "encoding", "page", "row", "group"
  };

  /** Natural-language-like sentence of 6-12 words drawn from a fixed vocabulary. */
  private static String sentence(Random r) {
    int words = 6 + r.nextInt(7);
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < words; i++) {
      if (i > 0) {
        sb.append(' ');
      }
      sb.append(WORDS[r.nextInt(WORDS.length)]);
    }
    return sb.toString();
  }
}
