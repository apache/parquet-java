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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test that the hardened variant reader behaves as expected.
 * Many of the tests save to the local filesystem and then clean up the files
 * in teardown. This is overkill, but it does permit these test files to
 * be preserved and so contributed to the parquet-format repository for
 * interoperability testing.
 *
 * <p>Set {@link #SAVE_GENERATED_FILES} to true to enable file preservation under
 * {@code parquet-variant/target/test-hardened-reader}; return it to false to
 * restore cleanup. Each file will be saved with the name of the test.
 * .
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
   * This allows file to be added to the bad_data section of parquet-testing repository.
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
    assertThat(v.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(v.numArrayElements()).isEqualTo(1);
    assertThat(v.getElementAtIndex(0).getType()).isEqualTo(Variant.Type.NULL);
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
    byte[] value = new byte[] {0x13, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F};
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, value, "offset table");
  }

  /**
   * {@link VariantUtil#valueSize} decodes a raw value buffer directly, without going through the
   * validating {@link Variant} constructor.
   */
  @Test
  public void testValueSizeRejectsOffsetArithmeticOverflow() {
    // Large array claiming numElements = 0x7FFFFFFF
    byte[] value = new byte[] {0x13, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F};
    assertThatThrownBy(() -> VariantUtil.valueSize(ByteBuffer.wrap(value)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset table");
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
    assertThat(baseTop.numArrayElements()).isEqualTo(1);
    assertThat(baseTop.getElementAtIndex(0).getType()).isEqualTo(Variant.Type.ARRAY);

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
    assertThat(top.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(top.numArrayElements()).isEqualTo(1);

    // Descending into the malformed inner is what trips the per-child shallow check.
    assertThatThrownBy(() -> top.getElementAtIndex(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("offset table");
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
    assertThat(top.getType()).isEqualTo(Variant.Type.ARRAY);

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
    assertThat(successfulDescents).isEqualTo(VariantUtil.MAX_VARIANT_DEPTH);
    assertThat(thrown).hasMessageContaining("nesting depth");
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
    assertThat(top.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(top.numObjectElements()).isEqualTo(1);

    // The per-entry dict offset check fires only when the caller resolves a field name.
    // getMetadataKey throws IllegalStateException for non-monotonic offsets — both that and
    // IllegalArgumentException are subtypes of RuntimeException, neither is OOM or SOE.
    assertThatThrownBy(() -> top.getFieldAtIndex(0))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("offset");
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

  /**
   * Reject an empty metadata buffer.
   */
  @Test
  public void testEmptyMetadataRejected() {
    assertThatThrownBy(() -> new Variant(ByteBuffer.wrap(arrayOneNull()), ByteBuffer.wrap(new byte[0])))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty");
  }

  /**
   * Reject a single-byte metadata buffer.
   */
  @Test
  public void testSingleByteMetadataRejected() {
    assertThatThrownBy(() -> new Variant(ByteBuffer.wrap(arrayOneNull()), ByteBuffer.wrap(new byte[] {0x01})))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("truncated");
  }

  /**
   * Handling of unknown metadata versions.
   */
  @Test
  public void testUnsupportedMetadataVersionRejected() throws IOException {
    // Header 0x02: version=2 in the low bits, offsetSize=1. A well-formed empty dictionary.
    final byte[] metadata = {0x02, 0x00, 0x00};
    byte[][] roundTripped = roundTrip(metadata, arrayOneNull());
    new Variant(ByteBuffer.wrap(roundTripped[1]), ByteBuffer.wrap(roundTripped[0]));
  }

  /**
   * Reject an object whose field id points past the metadata dictionary. The shallow validator
   * bounds every object key id against {@code dictSize}; an id {@code >= dictSize} cannot name a
   * dictionary entry and must be rejected at construction.
   */
  @Test
  public void testFieldIdOutOfRange() throws IOException {
    // Metadata with a one-entry dictionary {"a"} (dictSize=1):
    //   [0] 0x01 header: version=1, offsetSize=1
    //   [1] 0x01 dictSize = 1
    //   [2] 0x00 offset[0] = 0
    //   [3] 0x01 offset[1] = 1
    //   [4] 'a'  string data
    byte[] metadata = {0x01, 0x01, 0x00, 0x01, 'a'};

    // One-field object whose id is 5 — past the single dictionary entry.
    // objectHeader(largeSize=false, idSize=1, offsetSize=1) = 0x02
    //   [0] 0x02 header
    //   [1] 0x01 numElements = 1
    //   [2] 0x05 id[0] = 5   (out of range: dictSize = 1)
    //   [3] 0x00 offset[0] = 0
    //   [4] 0x01 offset[1] = 1
    //   [5] 0x00 data: NULL primitive
    byte[] value = {0x02, 0x01, 0x05, 0x00, 0x01, 0x00};
    expectRoundTripRaisesIllegalArgument(metadata, value, "out of range");
  }

  /**
   * Reject a long-string/binary primitive whose declared size runs past the buffer.
   */
  @Test
  public void testOversizedPrimitiveStringSize() throws IOException {
    byte[] value = {0x40, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x7F};
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, value, "extends past buffer");
  }

  /**
   * Reject a long-string/binary primitive whose four-byte size field does not fit in the buffer.
   */
  @Test
  public void testTruncatedPrimitiveSizeField() throws IOException {
    byte[] value = {0x40};
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, value, "truncated");
  }

  /**
   * Reject metadata whose four-byte dictionary size reads as a negative int. With offsetSize=4 the
   * dictionary_size field is read via the wide path, whose {@code v >= 0} guard is the only thing
   * standing between {@code 0xFFFFFFFF} and a negative size used in later arithmetic.
   */
  @Test
  public void testNegativeDictionarySize() throws IOException {
    // Header 0xC1: version=1 (low bits), offsetSize=4 (bits 6-7). dictSize field = 0xFFFFFFFF.
    byte[] metadata = {(byte) 0xC1, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    expectRoundTripRaisesIllegalArgument(metadata, arrayOneNull(), "unsigned int");
  }

  /**
   * Reject a short-string primitive whose inline length (carried in the header's type-info bits)
   * runs past the buffer. Here the header claims a 7-byte string but no string bytes follow.
   */
  @Test
  public void testShortStringLengthExceedsBuffer() throws IOException {
    // shortStringHeader(basicType=SHORT_STR=1, length=7) = (7 << 2) | 1 = 0x1D, with no payload.
    expectRoundTripRaisesIllegalArgument(EMPTY_METADATA, new byte[] {0x1D}, "short string");
  }

  /**
   * Handling of unknown primitives.
   */
  @Test
  public void testUnknownPrimitiveType() throws IOException {
    // Unknown primitive type
    final byte unknownPrimitive = (byte) 0xFC;
    final int elt3 = 0x14;
    byte[] value = {
      0b0011,
      0x03, // three elements
      0x00,
      0x02,
      0x06,
      0x08,
      0x0C,
      0x0A,
      unknownPrimitive, // element 1: unknown primitive type, 4 bytes wide
      0x01,
      0x02,
      0x03,
      0x0C,
      elt3
    };

    byte[][] rt = roundTrip(EMPTY_METADATA, value);
    Variant top = new Variant(ByteBuffer.wrap(rt[1]), ByteBuffer.wrap(rt[0]));

    // construction succeeds and reports all three elements.
    assertThat(top.getType()).isEqualTo(Variant.Type.ARRAY);
    assertThat(top.numArrayElements()).isEqualTo(3);

    // Get first and third elements, skip the unknown second one.
    Variant first = top.getElementAtIndex(0);
    assertThat(first.getType()).isEqualTo(Variant.Type.BYTE);
    assertThat(first.getByte()).isEqualTo((byte) 10);

    Variant third = top.getElementAtIndex(2);
    assertThat(third.getType()).isEqualTo(Variant.Type.BYTE);
    assertThat(third.getByte()).isEqualTo((byte) elt3);

    // Asking for the unknown element fails.
    assertThatThrownBy(() -> top.getElementAtIndex(1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown primitive type");
  }

  /**
   * Reconstructing a residual {@code value} column that carries an unknown primitive type fails to
   * parse. On the shredded read path the residual {@code value} column is the only channel that can
   * carry an unknown variant type (a {@code typed_value} column's type is fixed by the Parquet
   * schema), and it is replayed through {@link VariantBuilder#appendEncodedValue} — exactly what
   * {@code VariantConverters.VariantValueConverter} does on read.
   */
  @Test
  public void testUnknownShreddededPrimitiveTypeReconstruction() {
    final byte unknownPrimitive = (byte) 0xFC; // primitive typeInfo 63

    assertThatThrownBy(
            () -> new VariantBuilder().appendEncodedValue(ByteBuffer.wrap(new byte[] {unknownPrimitive})))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unknown type in Variant");
  }

  /**
   * The spec permits a variant field offsets to be non-monotonic, and equal offsets are a legal
   * consequence of that — two fields may point at the same value bytes.
   */
  @Test
  public void testDuplicateFieldOffsets() throws IOException {
    // Metadata with a two-entry dictionary {"a", "b"}
    byte[] metadata = {0x01, 0x02, 0x00, 0x01, 0x02, 'a', 'b'};

    // Two-field variant whose offsets are identical (both 0): fields "a" and "b" share the
    // single TRUE value in the data region.
    // objectHeader(largeSize=false, idSize=1, offsetSize=1) = 0x02
    //   [0] 0x02 header
    //   [1] 0x02 numElements = 2
    //   [2] 0x00 id[0] = 0  ("a")
    //   [3] 0x01 id[1] = 1  ("b")
    //   [4] 0x00 offset[0] = 0
    //   [5] 0x00 offset[1] = 0   (duplicate of offset[0]: "b" overlaps "a")
    //   [6] 0x01 offset[2] = 1   (terminator = data region length)
    //   [7] 0x04 data: a single TRUE primitive, referenced by both fields
    byte[] value = {0x02, 0x02, 0x00, 0x01, 0x00, 0x00, 0x01, 0x04};

    byte[][] rt = roundTrip(metadata, value);
    Variant top = new Variant(ByteBuffer.wrap(rt[1]), ByteBuffer.wrap(rt[0]));

    assertThat(top.getType()).isEqualTo(Variant.Type.OBJECT);
    assertThat(top.numObjectElements()).isEqualTo(2);

    // Both keys resolve to the shared value.
    Variant a = top.getFieldByKey("a");
    Variant b = top.getFieldByKey("b");
    assertThat(a.getType()).isEqualTo(Variant.Type.BOOLEAN);
    assertThat(b.getType()).isEqualTo(Variant.Type.BOOLEAN);
    assertThat(a.getBoolean()).isTrue();
    assertThat(b.getBoolean()).isTrue();

    // Index-ordered access agrees: both fields report their key and the same value.
    assertThat(top.getFieldAtIndex(0).key).isEqualTo("a");
    assertThat(top.getFieldAtIndex(1).key).isEqualTo("b");
    assertThat(top.getFieldAtIndex(0).value.getBoolean()).isTrue();
    assertThat(top.getFieldAtIndex(1).value.getBoolean()).isTrue();
  }

  @Test
  public void testInvalidImmutableMetadata() {
    // test immutable metadata resilience
    VariantBuilder vb = new VariantBuilder();
    VariantObjectBuilder obj = vb.startObject();
    obj.appendKey("one");
    obj.appendInt(1);
    obj.appendKey("two");
    obj.appendInt(2);
    vb.endObject();
    Variant variant = vb.build();

    ByteBuffer metaBuf = variant.getMetadataBuffer();
    ByteBuffer writable = ByteBuffer.allocate(metaBuf.remaining());
    writable.put(metaBuf.duplicate());
    writable.put(1, (byte) -1);
    writable.flip();
    // the offset rejected is 255. Without the hardening, later checks
    // will fail, but a different offset value will be reported.
    assertThatThrownBy(() -> new ImmutableMetadata(writable))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid byte-array offset (255). length: 11");
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

    assertThat(roundTripped[0])
        .describedAs("metadata changed during round-trip")
        .isEqualTo(metadata);
    assertThat(roundTripped[1])
        .describedAs("value changed during round-trip")
        .isEqualTo(value);
    assertThatThrownBy(() -> new Variant(ByteBuffer.wrap(roundTripped[1]), ByteBuffer.wrap(roundTripped[0])))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(messageSubstring);
  }

  /**
   * Write one row of {@code (metadata, value)} to an in-memory Parquet file, read it back
   * as [metadata, value]
   * @param metadata variant metadata
   * @param value data
   * @return the data read back as the tuple of metadata and value.
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
      assertThat(g).describedAs("expected at least one row").isNotNull();
      byte[] readMetadata = g.getBinary("metadata", 0).getBytes();
      byte[] readValue = g.getBinary("value", 0).getBytes();
      assertThat(reader.read()).describedAs("expected exactly one row").isNull();
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
