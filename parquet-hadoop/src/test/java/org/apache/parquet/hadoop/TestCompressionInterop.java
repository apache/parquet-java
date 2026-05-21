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
package org.apache.parquet.hadoop;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.airlift.compress.lzo.LzoCodec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end interoperability tests between the new direct compression path
 * (bypassing Hadoop codec abstraction) and the old Hadoop CompressionCodec path.
 *
 * <p>These tests verify that:
 * <ul>
 *   <li>Files written with the Hadoop codec path can be read correctly with the new direct path</li>
 *   <li>Files written with the new direct path can be read correctly with the Hadoop codec path</li>
 * </ul>
 *
 * <p>This ensures backward/forward compatibility of compressed Parquet files regardless
 * of which compression implementation produced them.
 *
 * <p>For LZO and BROTLI, the original Hadoop codec classes are not on the test classpath:
 * <ul>
 *   <li>LZO: Aircompressor's {@link io.airlift.compress.lzo.LzoCodec} (which implements
 *       {@link CompressionCodec} with Hadoop-compatible framing) is used as the Hadoop-path
 *       substitute, validating interop with the direct {@code LzoBytesCompressor}.</li>
 *   <li>BROTLI: A stream-based compressor/decompressor using brotli4j's
 *       {@code BrotliOutputStream}/{@code BrotliInputStream} is used to validate interop
 *       with the direct single-call {@code BrotliBytesCompressor}.</li>
 * </ul>
 */
public class TestCompressionInterop {

  private static final Logger LOG = LoggerFactory.getLogger(TestCompressionInterop.class);

  private static final int PAGE_SIZE = 64 * 1024;
  private static final int ROW_GROUP_SIZE = 256 * 1024;
  private static final int NUM_RECORDS = 500;

  private static final MessageType SCHEMA = parseMessageType("message test { "
      + "required binary binary_field; "
      + "required int32 int32_field; "
      + "required int64 int64_field; "
      + "required boolean boolean_field; "
      + "required float float_field; "
      + "required double double_field; "
      + "required fixed_len_byte_array(3) flba_field; "
      + "required int96 int96_field; "
      + "} ");

  /**
   * All codecs that have a direct bypass implementation in CodecFactory.
   * LZO uses aircompressor's LzoCodec (Hadoop-compatible) as the Hadoop-path substitute.
   * BROTLI uses brotli4j's streaming API as the Hadoop-path substitute.
   */
  private static final CompressionCodecName[] INTEROP_CODECS = {
    CompressionCodecName.SNAPPY,
    CompressionCodecName.GZIP,
    CompressionCodecName.ZSTD,
    CompressionCodecName.LZ4_RAW,
    CompressionCodecName.LZO,
    CompressionCodecName.BROTLI,
  };

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  // ---- Hadoop-path simulation factories ----

  /**
   * A CodecFactory that always uses the Hadoop CompressionCodec streaming path,
   * bypassing the optimized direct implementations. This simulates the behavior
   * before the GH-3530 optimization.
   *
   * <p>For LZO: uses aircompressor's {@link LzoCodec} which implements Hadoop's
   * {@link CompressionCodec} interface with Hadoop-compatible framing.
   *
   * <p>For BROTLI: uses a stream-based compressor/decompressor backed by brotli4j's
   * {@code BrotliOutputStream}/{@code BrotliInputStream} (via reflection since brotli4j
   * is a runtime-only dependency).
   */
  static class HadoopOnlyCodecFactory extends CodecFactory {
    HadoopOnlyCodecFactory(Configuration conf, int pageSize) {
      super(conf, pageSize);
    }

    @Override
    protected BytesCompressor createCompressor(CompressionCodecName codecName) {
      switch (codecName) {
        case UNCOMPRESSED:
          return NO_OP_COMPRESSOR;
        case LZO:
          // Use aircompressor's LzoCodec which implements Hadoop's CompressionCodec
          return new HeapBytesCompressor(codecName, new LzoCodec());
        case BROTLI:
          if (CodecFactory.Brotli4j.AVAILABLE) {
            return new BrotliStreamCompressor();
          }
          // fall through if brotli4j not available
        default:
          CompressionCodec codec = getCodec(codecName);
          if (codec == null) {
            return NO_OP_COMPRESSOR;
          }
          return new HeapBytesCompressor(codecName, codec);
      }
    }

    @Override
    protected BytesDecompressor createDecompressor(CompressionCodecName codecName) {
      switch (codecName) {
        case UNCOMPRESSED:
          return NO_OP_DECOMPRESSOR;
        case LZO:
          // Use aircompressor's LzoCodec which implements Hadoop's CompressionCodec
          return new HeapBytesDecompressor(new LzoCodec());
        case BROTLI:
          if (CodecFactory.Brotli4j.AVAILABLE) {
            return new BrotliStreamDecompressor();
          }
          // fall through if brotli4j not available
        default:
          CompressionCodec codec = getCodec(codecName);
          if (codec == null) {
            return NO_OP_DECOMPRESSOR;
          }
          return new HeapBytesDecompressor(codec);
      }
    }
  }

  /**
   * Stream-based Brotli compressor using brotli4j's BrotliOutputStream via reflection.
   * This mimics what a Hadoop BrotliCodec would do: wrap the output in a BrotliOutputStream.
   */
  static class BrotliStreamCompressor extends CodecFactory.BytesCompressor {
    private static final Constructor<?> BROTLI_OS_CTOR;

    static {
      Constructor<?> ctor = null;
      try {
        Class<?> bosClass = Class.forName("com.aayushatharva.brotli4j.encoder.BrotliOutputStream");
        ctor = bosClass.getConstructor(OutputStream.class);
      } catch (Throwable t) {
        // will be null — checked before use
      }
      BROTLI_OS_CTOR = ctor;
    }

    @Override
    public BytesInput compress(BytesInput bytes) throws IOException {
      try {
        ByteArrayOutputStream baos = new ByteArrayOutputStream((int) bytes.size());
        OutputStream bos = (OutputStream) BROTLI_OS_CTOR.newInstance(baos);
        bytes.writeAllTo(bos);
        bos.close();
        return BytesInput.from(baos.toByteArray());
      } catch (ReflectiveOperationException e) {
        throw new IOException("Brotli stream compression failed", e);
      }
    }

    @Override
    public CompressionCodecName getCodecName() {
      return CompressionCodecName.BROTLI;
    }

    @Override
    public void release() {}
  }

  /**
   * Stream-based Brotli decompressor using brotli4j's BrotliInputStream via reflection.
   * This mimics what a Hadoop BrotliCodec would do: wrap the input in a BrotliInputStream.
   */
  static class BrotliStreamDecompressor extends CodecFactory.BytesDecompressor {
    private static final Constructor<?> BROTLI_IS_CTOR;

    static {
      Constructor<?> ctor = null;
      try {
        Class<?> bisClass = Class.forName("com.aayushatharva.brotli4j.decoder.BrotliInputStream");
        ctor = bisClass.getConstructor(InputStream.class);
      } catch (Throwable t) {
        // will be null — checked before use
      }
      BROTLI_IS_CTOR = ctor;
    }

    @Override
    public BytesInput decompress(BytesInput bytes, int decompressedSize) throws IOException {
      try {
        InputStream bis = (InputStream) BROTLI_IS_CTOR.newInstance(bytes.toInputStream());
        byte[] output = new byte[decompressedSize];
        int offset = 0;
        while (offset < decompressedSize) {
          int read = bis.read(output, offset, decompressedSize - offset);
          if (read < 0) {
            throw new IOException(
                "Unexpected end of Brotli stream at offset " + offset + " of " + decompressedSize);
          }
          offset += read;
        }
        bis.close();
        return BytesInput.from(output);
      } catch (ReflectiveOperationException e) {
        throw new IOException("Brotli stream decompression failed", e);
      }
    }

    @Override
    public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int decompressedSize)
        throws IOException {
      byte[] compressed = new byte[compressedSize];
      input.get(compressed);
      BytesInput decompressed = decompress(BytesInput.from(compressed), decompressedSize);
      output.put(decompressed.toByteArray());
    }

    @Override
    public void release() {}
  }

  // ---- Write with Hadoop path, read with Direct path ----

  @Test
  public void writeHadoopReadDirect_SNAPPY() throws Exception {
    testWriteHadoopReadDirect(CompressionCodecName.SNAPPY);
  }

  @Test
  public void writeHadoopReadDirect_GZIP() throws Exception {
    testWriteHadoopReadDirect(CompressionCodecName.GZIP);
  }

  @Test
  public void writeHadoopReadDirect_ZSTD() throws Exception {
    testWriteHadoopReadDirect(CompressionCodecName.ZSTD);
  }

  @Test
  public void writeHadoopReadDirect_LZ4_RAW() throws Exception {
    testWriteHadoopReadDirect(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void writeHadoopReadDirect_LZO() throws Exception {
    testWriteHadoopReadDirect(CompressionCodecName.LZO);
  }

  @Test
  public void writeHadoopReadDirect_BROTLI() throws Exception {
    testWriteHadoopReadDirect(CompressionCodecName.BROTLI);
  }

  // ---- Write with Direct path, read with Hadoop path ----

  @Test
  public void writeDirectReadHadoop_SNAPPY() throws Exception {
    testWriteDirectReadHadoop(CompressionCodecName.SNAPPY);
  }

  @Test
  public void writeDirectReadHadoop_GZIP() throws Exception {
    testWriteDirectReadHadoop(CompressionCodecName.GZIP);
  }

  @Test
  public void writeDirectReadHadoop_ZSTD() throws Exception {
    testWriteDirectReadHadoop(CompressionCodecName.ZSTD);
  }

  @Test
  public void writeDirectReadHadoop_LZ4_RAW() throws Exception {
    testWriteDirectReadHadoop(CompressionCodecName.LZ4_RAW);
  }

  @Test
  public void writeDirectReadHadoop_LZO() throws Exception {
    testWriteDirectReadHadoop(CompressionCodecName.LZO);
  }

  @Test
  public void writeDirectReadHadoop_BROTLI() throws Exception {
    testWriteDirectReadHadoop(CompressionCodecName.BROTLI);
  }

  // ---- Bidirectional test: both directions for all codecs ----

  @Test
  public void bidirectionalInteropAllCodecs() throws Exception {
    for (CompressionCodecName codec : INTEROP_CODECS) {
      LOG.info("Testing bidirectional interop for codec: {}", codec);
      testWriteHadoopReadDirect(codec);
      testWriteDirectReadHadoop(codec);
    }
  }

  // ---- Multi-row-group test to validate interop across row group boundaries ----

  @Test
  public void writeHadoopReadDirect_multiRowGroup() throws Exception {
    for (CompressionCodecName codec : INTEROP_CODECS) {
      testInteropMultiRowGroup(codec, /* writeWithHadoop= */ true);
    }
  }

  @Test
  public void writeDirectReadHadoop_multiRowGroup() throws Exception {
    for (CompressionCodecName codec : INTEROP_CODECS) {
      testInteropMultiRowGroup(codec, /* writeWithHadoop= */ false);
    }
  }

  // ---- Implementation ----

  private void testWriteHadoopReadDirect(CompressionCodecName codec) throws Exception {
    Configuration conf = new Configuration();
    Path file = tempFolder.newFolder().toPath().resolve("hadoop_write_" + codec.name() + ".parquet");

    // Write using Hadoop codec path
    CompressionCodecFactory hadoopFactory = new HadoopOnlyCodecFactory(conf, PAGE_SIZE);
    List<Group> expectedRecords = writeFile(file, codec, hadoopFactory);
    hadoopFactory.release();

    // Read using direct (default) codec path
    CodecFactory directFactory = new CodecFactory(conf, PAGE_SIZE);
    List<Group> actualRecords = readFile(file, directFactory);
    directFactory.release();

    // Verify
    assertRecordsEqual(expectedRecords, actualRecords, codec.name() + " hadoop->direct");
  }

  private void testWriteDirectReadHadoop(CompressionCodecName codec) throws Exception {
    Configuration conf = new Configuration();
    Path file = tempFolder.newFolder().toPath().resolve("direct_write_" + codec.name() + ".parquet");

    // Write using direct (default) codec path
    CodecFactory directFactory = new CodecFactory(conf, PAGE_SIZE);
    List<Group> expectedRecords = writeFile(file, codec, directFactory);
    directFactory.release();

    // Read using Hadoop codec path
    CompressionCodecFactory hadoopFactory = new HadoopOnlyCodecFactory(conf, PAGE_SIZE);
    List<Group> actualRecords = readFile(file, hadoopFactory);
    hadoopFactory.release();

    // Verify
    assertRecordsEqual(expectedRecords, actualRecords, codec.name() + " direct->hadoop");
  }

  private void testInteropMultiRowGroup(CompressionCodecName codec, boolean writeWithHadoop) throws Exception {
    Configuration conf = new Configuration();
    String prefix = writeWithHadoop ? "hadoop_mrg_" : "direct_mrg_";
    Path file = tempFolder.newFolder().toPath().resolve(prefix + codec.name() + ".parquet");

    // Use a small row group size to force multiple row groups
    int smallRowGroupSize = 4 * 1024;

    CompressionCodecFactory writeFactory =
        writeWithHadoop ? new HadoopOnlyCodecFactory(conf, PAGE_SIZE) : new CodecFactory(conf, PAGE_SIZE);
    CompressionCodecFactory readFactory =
        writeWithHadoop ? new CodecFactory(conf, PAGE_SIZE) : new HadoopOnlyCodecFactory(conf, PAGE_SIZE);

    List<Group> expectedRecords = writeFile(file, codec, writeFactory, smallRowGroupSize, 1000);
    writeFactory.release();

    List<Group> actualRecords = readFile(file, readFactory);
    readFactory.release();

    assertRecordsEqual(expectedRecords, actualRecords, codec.name() + " multi-row-group " + prefix);
  }

  private List<Group> writeFile(Path file, CompressionCodecName codec, CompressionCodecFactory factory)
      throws IOException {
    return writeFile(file, codec, factory, ROW_GROUP_SIZE, NUM_RECORDS);
  }

  private List<Group> writeFile(
      Path file, CompressionCodecName codec, CompressionCodecFactory factory, int rowGroupSize, int numRecords)
      throws IOException {
    SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
    List<Group> records = generateRecords(groupFactory, numRecords);

    OutputFile outputFile = new LocalOutputFile(file);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
        .withType(SCHEMA)
        .withCompressionCodec(codec)
        .withCodecFactory(factory)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(PAGE_SIZE)
        .withWriteMode(ParquetFileWriter.Mode.CREATE)
        .build()) {
      for (Group record : records) {
        writer.write(record);
      }
    }

    return records;
  }

  private List<Group> readFile(Path file, CompressionCodecFactory factory) throws IOException {
    List<Group> records = new ArrayList<>();
    InputFile inputFile = new LocalInputFile(file);
    try (ParquetReader<Group> reader = new ParquetReader.Builder<Group>(inputFile) {
      @Override
      protected ReadSupport<Group> getReadSupport() {
        return new GroupReadSupport();
      }
    }.withCodecFactory(factory).build()) {
      Group record;
      while ((record = reader.read()) != null) {
        records.add(record);
      }
    }
    return records;
  }

  private List<Group> generateRecords(SimpleGroupFactory factory, int numRecords) {
    List<Group> records = new ArrayList<>(numRecords);
    Random random = new Random(42); // fixed seed for reproducibility

    for (int i = 0; i < numRecords; i++) {
      byte[] binaryData = new byte[10 + random.nextInt(50)];
      random.nextBytes(binaryData);

      byte[] flbaData = new byte[3];
      random.nextBytes(flbaData);

      byte[] int96Data = new byte[12];
      random.nextBytes(int96Data);

      records.add(factory.newGroup()
          .append("binary_field", Binary.fromConstantByteArray(binaryData))
          .append("int32_field", random.nextInt())
          .append("int64_field", random.nextLong())
          .append("boolean_field", random.nextBoolean())
          .append("float_field", random.nextFloat())
          .append("double_field", random.nextDouble())
          .append("flba_field", Binary.fromConstantByteArray(flbaData))
          .append("int96_field", Binary.fromConstantByteArray(int96Data)));
    }
    return records;
  }

  private void assertRecordsEqual(List<Group> expected, List<Group> actual, String context) {
    assertEquals("Record count mismatch for " + context, expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Group exp = expected.get(i);
      Group act = actual.get(i);
      String msg = context + " record " + i;

      assertArrayEquals(
          msg + " binary_field",
          exp.getBinary("binary_field", 0).getBytes(),
          act.getBinary("binary_field", 0).getBytes());
      assertEquals(msg + " int32_field", exp.getInteger("int32_field", 0), act.getInteger("int32_field", 0));
      assertEquals(msg + " int64_field", exp.getLong("int64_field", 0), act.getLong("int64_field", 0));
      assertEquals(
          msg + " boolean_field", exp.getBoolean("boolean_field", 0), act.getBoolean("boolean_field", 0));
      assertEquals(msg + " float_field", exp.getFloat("float_field", 0), act.getFloat("float_field", 0), 0.0f);
      assertEquals(
          msg + " double_field", exp.getDouble("double_field", 0), act.getDouble("double_field", 0), 0.0d);
      assertArrayEquals(
          msg + " flba_field",
          exp.getBinary("flba_field", 0).getBytes(),
          act.getBinary("flba_field", 0).getBytes());
      assertArrayEquals(
          msg + " int96_field",
          exp.getInt96("int96_field", 0).getBytes(),
          act.getInt96("int96_field", 0).getBytes());
    }
  }
}
