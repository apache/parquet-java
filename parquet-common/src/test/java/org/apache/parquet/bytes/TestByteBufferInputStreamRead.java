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

package org.apache.parquet.bytes;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests {@link ByteBufferInputStream#read(ByteBuffer)} appending into the caller's destination buffer for both single
 * buffer and multi buffer streams created through the public wrap entry points.
 */
public class TestByteBufferInputStreamRead {

  private static final byte PREFIX_BYTE = (byte) 0x11;
  private static final byte SUFFIX_BYTE = (byte) 0x77;
  private static final int PREFIX_LENGTH = 3;
  private static final int DEST_CAPACITY = 16;
  private static final int DEST_LIMIT = 8;
  private static final int SLICE_MARGIN = 4;

  private enum OutputKind {
    HEAP,
    DIRECT,
    SLICED
  }

  private enum SourceKind {
    SINGLE,
    MULTI
  }

  static Stream<Arguments> kinds() {
    return Stream.of(OutputKind.values())
        .flatMap(output -> Stream.of(SourceKind.values()).map(source -> Arguments.of(output, source)));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("kinds")
  public void partialCopyAdvancesDestinationPosition(OutputKind output, SourceKind source) {
    byte[] data = data(10, 1);
    int copied = DEST_LIMIT - PREFIX_LENGTH;
    ByteBuffer dest = destination(output, DEST_LIMIT, PREFIX_LENGTH);

    assertRead(output + " " + source, stream(source, data, false), dest, copied);

    assertContent(dest, output, source, Arrays.copyOf(data, copied));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("kinds")
  public void fullSourceCopyKeepsLimitAndRoomForAppendedRead(OutputKind output, SourceKind source) {
    byte[] first = data(4, 1);
    byte[] second = data(3, 11);
    ByteBuffer dest = destination(output, DEST_CAPACITY, PREFIX_LENGTH);

    assertRead(output + " " + source + " first read", stream(source, first, true), dest, first.length);
    assertRead(output + " " + source + " second read", stream(source, second, true), dest, second.length);

    assertContent(dest, output, source, first, second);
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("kinds")
  public void readIntoExhaustedDestinationReturnsZeroWithoutMutatingIt(OutputKind output, SourceKind source) {
    byte[] data = data(10, 1);
    ByteBuffer dest = destination(output, DEST_LIMIT, DEST_LIMIT);
    ByteBufferInputStream in = stream(source, data, false);

    assertThat(in.read(dest)).as("%s %s: bytes copied", output, source).isZero();
    assertThat(dest.position())
        .as("%s %s: destination position", output, source)
        .isEqualTo(DEST_LIMIT);
    assertThat(dest.limit()).as("%s %s: destination limit", output, source).isEqualTo(DEST_LIMIT);
    assertThat(in.position()).as("%s %s: source position", output, source).isZero();
    assertThat(in.available()).as("%s %s: source remaining", output, source).isEqualTo(data.length);
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("kinds")
  public void readAtEndOfStreamReturnsMinusOneWithoutMutatingDestination(OutputKind output, SourceKind source)
      throws Exception {
    byte[] data = data(10, 1);
    ByteBufferInputStream in = stream(source, data, true);
    ByteBuffer dest = destination(output, DEST_LIMIT, PREFIX_LENGTH);

    assertThat(in.read(new byte[data.length]))
        .as("%s %s: drained bytes", output, source)
        .isEqualTo(data.length);
    assertThat(in.read(dest)).as("%s %s: bytes copied", output, source).isEqualTo(-1);
    assertThat(dest.position())
        .as("%s %s: destination position", output, source)
        .isEqualTo(PREFIX_LENGTH);
    assertThat(dest.limit()).as("%s %s: destination limit", output, source).isEqualTo(DEST_LIMIT);
    assertThat(in.position()).as("%s %s: source position", output, source).isEqualTo(data.length);
    assertThat(in.available()).as("%s %s: source remaining", output, source).isZero();
  }

  private static void assertRead(String description, ByteBufferInputStream in, ByteBuffer dest, int expectedCopied) {
    int destStart = dest.position();
    int destLimit = dest.limit();
    long sourceStart = in.position();
    int sourceRemaining = in.available();

    assertThat(in.read(dest)).as("%s: bytes copied", description).isEqualTo(expectedCopied);
    assertThat(dest.position()).as("%s: destination position", description).isEqualTo(destStart + expectedCopied);
    assertThat(dest.limit()).as("%s: destination limit", description).isEqualTo(destLimit);
    assertThat(in.position()).as("%s: source position", description).isEqualTo(sourceStart + expectedCopied);
    assertThat(in.available()).as("%s: source remaining", description).isEqualTo(sourceRemaining - expectedCopied);
  }

  private static void assertContent(ByteBuffer dest, OutputKind output, SourceKind source, byte[]... payloads) {
    byte[] expected = new byte
        [PREFIX_LENGTH
            + Arrays.stream(payloads)
                .mapToInt(payload -> payload.length)
                .sum()];
    Arrays.fill(expected, 0, PREFIX_LENGTH, PREFIX_BYTE);
    int offset = PREFIX_LENGTH;
    for (byte[] payload : payloads) {
      System.arraycopy(payload, 0, expected, offset, payload.length);
      offset += payload.length;
    }

    dest.flip();
    byte[] actual = new byte[dest.remaining()];
    dest.get(actual);
    assertThat(actual)
        .as("%s %s: prefilled prefix followed by the copied source bytes", output, source)
        .containsExactly(expected);

    ByteBuffer fullDestination = dest.duplicate();
    fullDestination.clear();
    for (int i = dest.limit(); i < dest.capacity(); i++) {
      assertThat(fullDestination.get(i))
          .as("%s %s: untouched destination byte %d", output, source, i)
          .isEqualTo(SUFFIX_BYTE);
    }
  }

  private static ByteBuffer destination(OutputKind output, int limit, int prefixLength) {
    ByteBuffer dest = ByteBuffer.allocate(DEST_CAPACITY);
    if (output == OutputKind.DIRECT) {
      dest = ByteBuffer.allocateDirect(DEST_CAPACITY);
    } else if (output == OutputKind.SLICED) {
      ByteBuffer parent = ByteBuffer.allocate(DEST_CAPACITY + 2 * SLICE_MARGIN);
      parent.position(SLICE_MARGIN);
      parent.limit(SLICE_MARGIN + DEST_CAPACITY);
      dest = parent.slice();
    }

    for (int i = 0; i < dest.capacity(); i++) {
      dest.put(i, SUFFIX_BYTE);
    }
    for (int i = 0; i < prefixLength; i++) {
      dest.put(i, PREFIX_BYTE);
    }
    dest.limit(limit);
    dest.position(prefixLength);

    return dest;
  }

  private static ByteBufferInputStream stream(SourceKind source, byte[] data, boolean viaList) {
    if (source == SourceKind.SINGLE) {
      ByteBuffer whole = ByteBuffer.wrap(data);
      return viaList ? ByteBufferInputStream.wrap(List.of(whole)) : ByteBufferInputStream.wrap(whole);
    }

    // the first buffer holds more than half of the data so that a small destination forces a partial copy
    int split = data.length / 2 + 1;
    ByteBuffer first = ByteBuffer.wrap(data, 0, split);
    ByteBuffer second = ByteBuffer.wrap(data, split, data.length - split);
    return viaList ? ByteBufferInputStream.wrap(List.of(first, second)) : ByteBufferInputStream.wrap(first, second);
  }

  private static byte[] data(int length, int firstValue) {
    byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (firstValue + i);
    }
    return data;
  }
}
