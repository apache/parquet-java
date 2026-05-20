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
package org.apache.parquet.variant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Exercises the bounds checks added to {@link Variant} construction by round-tripping a row of
 * {@code (metadata, value)} binaries through an in-memory uncompressed Parquet file, mutating
 * specific bytes after writing, and asserting that the resulting buffer is rejected with an
 * {@link IllegalArgumentException}.
 */
public class TestHardenedReader {

  private static final String SCHEMA_STRING =
      "message variant_record { required binary metadata; required binary value; }";

  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(SCHEMA_STRING);

  /**
   * Minimal well-formed empty metadata: version=1, offsetSize=1, dictSize=0, single end-offset=0.
   */
  private static final byte[] EMPTY_METADATA = {0x01, 0x00, 0x00};

  /**
   * Set to {@code true} to persist the generated Parquet files under
   * {@code target/test-hardened-reader/$testName.parquet}.
   */
  private static final boolean SAVE_GENERATED_FILES = false;

  /** Directory under the module's build dir where generated files are written, if enabled. */
  private static final Path OUTPUT_DIR = Paths.get("target", "test-hardened-reader");

  @Rule
  public TestName testName = new TestName();

  /**
   * Happy path.
   */
  @Test
  public void testWellFormedRoundTrip() throws IOException {
    byte[] wellFormed = arrayOneNull();
    byte[][] roundTripped = roundTrip(EMPTY_METADATA, wellFormed);
    Variant v = new Variant(ByteBuffer.wrap(roundTripped[1]), ByteBuffer.wrap(roundTripped[0]));
    assertEquals(Variant.Type.ARRAY, v.getType());
    assertEquals(1, v.numArrayElements());
    assertEquals(Variant.Type.NULL, v.getElementAtIndex(0).getType());
  }

  /**
   * Out-of-range child offset.
   */
  @Test
  public void testOutOfRangeChildOffset() throws IOException {
    byte[] value = arrayOneNull();
    // Layout of arrayOneNull(): [header, numElements=1, offset[0]=0, offset[1]=1, NULL_header]
    // Set offset[0] to 0xFF — far outside the 1-byte data region.
    value[2] = (byte) 0xFF;
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, value, "child offset");
  }

  /**
   * Reject numElements larger than the surrounding buffer.
   */
  @Test
  public void testOversizedNumElements() throws IOException {
    // Use a largeSize array so numElements is a 4-byte field we can grow.
    // Layout: [arrayHeader(large=1,offsetSize=1)=0b10011, numElements(4 bytes LE)=0,
    //          end-offset(1 byte)=0]   — total 6 bytes, zero data.
    byte[] value = new byte[] {0b10011, 0x00, 0x00, 0x00, 0x00, 0x00};
    // Baseline must parse cleanly.
    new Variant(ByteBuffer.wrap(value.clone()), ByteBuffer.wrap(EMPTY_METADATA.clone()));
    // Set numElements to a value whose offset table cannot fit in the buffer.
    int huge = 0x10000000;
    value[1] = (byte) (huge & 0xFF);
    value[2] = (byte) ((huge >> 8) & 0xFF);
    value[3] = (byte) ((huge >> 16) & 0xFF);
    value[4] = (byte) ((huge >> 24) & 0xFF);
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, value, "offset table");
  }

  /**
   * Reject Offset arithmetic that would wrap java's signed 32-bit integer.
   */
  @Test
  public void testOffsetArithmeticBoundsCheck() throws IOException {
    // Object header that claims numElements and offsetSize values whose product overflows int
    // unless validated with widened arithmetic.
    // Layout: [objectHeader(large=1, idSize=1, offsetSize=4) = 0x4E,
    //          numElements (4 bytes, little-endian) = 0x33333333,
    //          three filler bytes]
    byte[] value = new byte[] {0x4E, 0x33, 0x33, 0x33, 0x33, (byte) 0xFF, (byte) 0xFF, 0x3F};
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, value, "");
  }

  /**
   * Well-formed outer array containing a malformed inner array: the outer header table is
   * consistent with the buffer extent and is accepted at top-level construction, but the inner
   * child is rejected only when the caller descends into it.
   */
  @Test
  public void testMalformedChildInsideWellFormedParent() throws IOException {
    // Outer layout (offsetSize=1, smallSize, 1 element):
    //   [0] 0b0011  arrayHeader(largeSize=false, offsetSize=1)
    //   [1] 0x01    numElements = 1
    //   [2] 0x00    offset[0]  = 0
    //   [3] 0x06    offset[1]  = 6  (terminator = data region length)
    //   [4..9]      6-byte child data region
    //
    // Inner layout, occupying that 6-byte region:
    //   [4] 0b10011 arrayHeader(largeSize=true, offsetSize=1)
    //   [5..8]      4-byte LE numElements
    //   [9] 0x00    end-offset
    byte[] value = {
      0b0011,
      0x01,
      0x00,
      0x06, // outer header + offset table
      0b10011,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00 // inner: well-formed baseline (numElements=0)
    };

    // Baseline: well-formed outer + inner round-trip cleanly and descend without complaint.
    byte[][] baseline = roundTrip(EMPTY_METADATA, value.clone());
    Variant baseTop = new Variant(ByteBuffer.wrap(baseline[1]), ByteBuffer.wrap(baseline[0]));
    assertEquals(1, baseTop.numArrayElements());
    assertEquals(Variant.Type.ARRAY, baseTop.getElementAtIndex(0).getType());

    // Patch the inner numElements field to a value whose offset table cannot fit in the
    // 6-byte inner slot. The outer's own header table is unchanged.
    int huge = 0x10000000;
    value[5] = (byte) (huge & 0xFF);
    value[6] = (byte) ((huge >> 8) & 0xFF);
    value[7] = (byte) ((huge >> 16) & 0xFF);
    value[8] = (byte) ((huge >> 24) & 0xFF);

    byte[][] rt = roundTrip(EMPTY_METADATA, value);
    Variant top = new Variant(ByteBuffer.wrap(rt[1]), ByteBuffer.wrap(rt[0]));
    // Top-level construction succeeds — the outer is well-formed.
    assertEquals(Variant.Type.ARRAY, top.getType());
    assertEquals(1, top.numArrayElements());

    // Descending into the malformed inner is what trips the per-child shallow check.
    IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> top.getElementAtIndex(0));
    assertExceptionMessageContains(thrown, "offset table");
  }

  /**
   * Deeply-nested chain of single-element arrays. Each container is well-formed in isolation
   * (the byte layout strictly shrinks at each level, so per-level shallow validation accepts
   * every node). A naive recursive consumer would descend indefinitely and exhaust the JVM
   * stack; the depth counter on the child constructor must reject the descent that would
   * cross {@link VariantUtil#MAX_VARIANT_DEPTH}.
   */
  @Test
  public void testOverdeepChainRejected() throws IOException {
    int arrayLevels = VariantUtil.MAX_VARIANT_DEPTH + 10;
    byte[] value = buildNestedArrayChain(arrayLevels);
    byte[][] rt = roundTrip(EMPTY_METADATA, value);
    Variant top = new Variant(ByteBuffer.wrap(rt[1]), ByteBuffer.wrap(rt[0]));
    assertEquals(Variant.Type.ARRAY, top.getType());

    // Descend until the depth check fires. The cursor at depth d is the variant produced by
    // d successful getElementAtIndex(0) calls. The (MAX_VARIANT_DEPTH + 1)-th call attempts
    // to construct a child at depth MAX_VARIANT_DEPTH + 1, which is rejected.
    Variant cursor = top;
    int successfulDescents = 0;
    IllegalArgumentException thrown = null;
    while (thrown == null) {
      try {
        cursor = cursor.getElementAtIndex(0);
        successfulDescents++;
      } catch (IllegalArgumentException e) {
        thrown = e;
      }
    }
    assertEquals(VariantUtil.MAX_VARIANT_DEPTH, successfulDescents);
    String msg = thrown.getMessage();
    assertTrue(
        "Expected message to mention nesting depth: " + msg,
        msg != null && msg.toLowerCase().contains("nesting depth"));
  }

  /**
   * Build a value buffer consisting of {@code arrayLevels} single-element arrays nested one
   * inside the next, with a {@code NULL} primitive at the bottom. Each array uses
   * {@code offsetSize=2}, smallSize, so the per-level header + offset table is 6 bytes.
   */
  private static byte[] buildNestedArrayChain(int arrayLevels) {
    // arrayHeader(largeSize=false, offsetSize=2) =
    //   (0 << 4) | ((2-1) << 2) | ARRAY(3) = 0b0111
    final byte arrayHeader = 0b0111;
    final int perLevel = 6; // 1 header + 1 numElements + 2 offsets * 2 bytes each
    final int leafSize = 1; // NULL primitive header
    int total = perLevel * arrayLevels + leafSize;
    byte[] buf = new byte[total];
    for (int i = 0; i < arrayLevels; i++) {
      int pos = i * perLevel;
      int dataLen = total - pos - perLevel; // size of the inner container at this level
      buf[pos] = arrayHeader;
      buf[pos + 1] = 0x01; // numElements = 1
      buf[pos + 2] = 0x00; // offset[0] LE byte 0 (= 0)
      buf[pos + 3] = 0x00; // offset[0] LE byte 1
      buf[pos + 4] = (byte) (dataLen & 0xFF); // offset[1] LE byte 0
      buf[pos + 5] = (byte) ((dataLen >> 8) & 0xFF); // offset[1] LE byte 1
    }
    buf[total - 1] = (byte) 0x00; // NULL primitive header
    return buf;
  }

  /**
   * Metadata whose dictionary table fits in the buffer (so top-level construction accepts it)
   * but whose per-entry offsets are non-monotonic. The structural fit-check is eager; the
   * per-entry offset check is deferred to {@link VariantUtil#getMetadataKey}, which fires
   * when a caller actually reads a field name.
   */
  @Test
  public void testMalformedDictOffsetCaughtLazily() throws IOException {
    // Metadata layout (offsetSize=1, dictSize=2):
    //   [0] 0x01    header: version=1, offsetSize=1
    //   [1] 0x02    dictSize = 2  (table fits: 1 + 1 + 3*1 = 5 ≤ buffer size)
    //   [2] 0x00    offset[0] = 0
    //   [3] 0x05    offset[1] = 5   (entry 0 spans data bytes [0..5))
    //   [4] 0x03    offset[2] = 3   (BOGUS: entry 1 would have negative span)
    //   [5..9]      5 bytes of string data
    byte[] metadata = {0x01, 0x02, 0x00, 0x05, 0x03, 'A', 'B', 'C', 'D', 'E'};

    // Value: 1-field object referencing dict id 1.
    // objectHeader(largeSize=false, idSize=1, offsetSize=1) = 0x02
    //   [0] 0x02   header
    //   [1] 0x01   numElements = 1
    //   [2] 0x01   id[0] = 1  (the bogus dict entry)
    //   [3] 0x00   offset[0] = 0
    //   [4] 0x01   offset[1] = 1
    //   [5] 0x00   data: NULL primitive
    byte[] value = {0x02, 0x01, 0x01, 0x00, 0x01, 0x00};

    byte[][] rt = roundTrip(metadata, value);
    Variant top = new Variant(ByteBuffer.wrap(rt[1]), ByteBuffer.wrap(rt[0]));
    // Top-level construction succeeds: metadata table fits, value is shallow-valid, id < dictSize.
    assertEquals(Variant.Type.OBJECT, top.getType());
    assertEquals(1, top.numObjectElements());

    // The per-entry dict offset check fires only when the caller resolves a field name.
    // getMetadataKey throws IllegalStateException for non-monotonic offsets — both that and
    // IllegalArgumentException are subtypes of RuntimeException, neither is OOM or SOE.
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> top.getFieldAtIndex(0));
    final String expected = "offset";
    assertExceptionMessageContains(thrown, expected);
  }

  /**
   * Reject Metadata dictSize larger than the surrounding buffer.
   */
  @Test
  public void testOversizedMetadataDictSize() throws IOException {
    // Start from the empty metadata, then set dictSize to 0xFF.
    // With offsetSize=1 the offset table would then occupy 256 bytes — past the 3-byte buffer.
    byte[] metadata = EMPTY_METADATA.clone();
    metadata[1] = (byte) 0xFF;
    expectRoundTripRaisesIllegalArgument(metadata, arrayOneNull(), "dictionary");
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  /** Single-element array containing a NULL primitive. 5 bytes. */
  private static byte[] arrayOneNull() {
    // header = arrayHeader(largeSize=false, offsetSize=1) = (0 << 4) | (0 << 2) | ARRAY = 0b0011
    // numElements = 1; offset[0] = 0, offset[1] = 1; data: NULL primitive header = 0x00.
    return new byte[] {0b0011, 0x01, 0x00, 0x01, 0x00};
  }

  /**
   * Run patched bytes through a real Parquet write/read cycle and expect rejection.
   * @param metadata  metadata
   * @param value payload
   * @param messageSubstring substring of expected error message. Pass "" to skip check.
   * */
  private void expectRoundTripRaisesIllegalArgument(byte[] metadata, byte[] value, String messageSubstring)
      throws IOException {
    byte[][] roundTripped = roundTrip(metadata, value);
    Assert.assertArrayEquals("metadata changed during round-trip", metadata, roundTripped[0]);
    Assert.assertArrayEquals("value changed during round-trip", value, roundTripped[1]);
    IllegalArgumentException thrown = assertThrows(
        IllegalArgumentException.class,
        () -> new Variant(ByteBuffer.wrap(roundTripped[1]), ByteBuffer.wrap(roundTripped[0])));
    assertExceptionMessageContains(thrown, messageSubstring);
  }

  /**
   * Assert that an exception contains a specific message. If it doesn't, an assertion is raised that
   * contains the thrown exception too.
   * @param thrown exception to be analyzed.
   * @param expected expected string, case-insensitive.
   * @throws AssertionError is the text isn't found in the exception message.
   */
  private static void assertExceptionMessageContains(RuntimeException thrown, String expected) {

    String msg = thrown.getMessage();
    if (msg == null || !msg.toLowerCase().contains(expected)) {
      throw new AssertionError("Did not find \"" + expected + "\" in: " + msg, thrown);
    }
  }

  /**
   * Write one row of {@code (metadata, value)} to an in-memory Parquet file, read it back.
   * @param metadata variant metadata
   * @param value data
   * @return the data read back
   */
  private byte[][] roundTrip(byte[] metadata, byte[] value) throws IOException {
    ByteArrayOutputFile out = new ByteArrayOutputFile();

    // write it out
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(out)
        .withType(SCHEMA)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withDictionaryEncoding(false)
        .build()) {
      SimpleGroup row = new SimpleGroup(SCHEMA);
      row.add("metadata", Binary.fromConstantByteArray(metadata));
      row.add("value", Binary.fromConstantByteArray(value));
      writer.write(row);
    }

    byte[] parquetBytes = out.toByteArray();

    // optional save to fs for later testing.
    if (SAVE_GENERATED_FILES) {
      Files.createDirectories(OUTPUT_DIR);
      Path target = OUTPUT_DIR.resolve(testName.getMethodName() + ".parquet");
      Files.write(target, parquetBytes);
    }
    // read it back
    ByteArrayInputFile in = new ByteArrayInputFile(parquetBytes);
    try (ParquetReader<Group> reader = new GroupParquetReaderBuilder(in).build()) {
      Group g = reader.read();
      Assert.assertNotNull("expected at least one row", g);
      byte[] readMetadata = g.getBinary("metadata", 0).getBytes();
      byte[] readValue = g.getBinary("value", 0).getBytes();
      Assert.assertNull("expected exactly one row", reader.read());
      return new byte[][] {readMetadata, readValue};
    }
  }

  /**
   * ParquetReader.Builder<Group> over an InputFile, using GroupReadSupport.
   */
  private static final class GroupParquetReaderBuilder extends ParquetReader.Builder<Group> {
    GroupParquetReaderBuilder(InputFile file) {
      super(file);
    }

    @Override
    protected ReadSupport<Group> getReadSupport() {
      return new GroupReadSupport();
    }
  }

  /** {@link OutputFile} backed by a {@link ByteArrayOutputStream}. */
  private static final class ByteArrayOutputFile implements OutputFile {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    byte[] toByteArray() {
      return baos.toByteArray();
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      return newStream();
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return newStream();
    }

    private PositionOutputStream newStream() {
      return new PositionOutputStream() {
        private long pos = 0;

        @Override
        public void write(int b) {
          baos.write(b);
          pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) {
          baos.write(b, off, len);
          pos += len;
        }

        @Override
        public long getPos() {
          return pos;
        }
      };
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  /** {@link InputFile} backed by a {@code byte[]}. */
  private static final class ByteArrayInputFile implements InputFile {
    private final byte[] data;

    ByteArrayInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new SeekableInputStream() {
        private int pos = 0;

        @Override
        public int read() {
          return pos < data.length ? (data[pos++] & 0xFF) : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
          int remaining = data.length - pos;
          if (remaining <= 0) {
            return -1;
          }
          int n = Math.min(len, remaining);
          System.arraycopy(data, pos, b, off, n);
          pos += n;
          return n;
        }

        @Override
        public long getPos() {
          return pos;
        }

        @Override
        public void seek(long newPos) {
          pos = (int) newPos;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
          readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
          if (pos + len > data.length) {
            throw new EOFException("Unexpected end of data");
          }
          System.arraycopy(data, pos, bytes, start, len);
          pos += len;
        }

        @Override
        public int read(ByteBuffer buf) {
          int len = buf.remaining();
          int remaining = data.length - pos;
          if (remaining <= 0) {
            return -1;
          }
          int n = Math.min(len, remaining);
          buf.put(data, pos, n);
          pos += n;
          return n;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
          int len = buf.remaining();
          if (pos + len > data.length) {
            throw new IOException("Unexpected end of data");
          }
          buf.put(data, pos, len);
          pos += len;
        }

        @Override
        public void close() {}
      };
    }
  }
}
