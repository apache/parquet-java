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
package org.apache.parquet.column.values.deltalengthbytearray;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

public class TestDeltaLengthByteArray {

  String[] values = {"parquet", "hadoop", "mapreduce"};

  private DeltaLengthByteArrayValuesWriter getDeltaLengthByteArrayValuesWriter() {
    return new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
  }

  @Test
  public void testSerialization() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), values.length);

    for (int i = 0; i < bin.length; i++) {
      assertThat(bin[i]).isEqualTo(Binary.fromString(values[i]));
    }
  }

  @Test
  public void testRandomStrings() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    String[] values = Utils.getRandomStringSamples(1000, 32);
    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), values.length);

    for (int i = 0; i < bin.length; i++) {
      assertThat(bin[i]).isEqualTo(Binary.fromString(values[i]));
    }
  }

  @Test
  public void testSkipWithRandomStrings() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    String[] values = Utils.getRandomStringSamples(1000, 32);
    Utils.writeData(writer, values);

    reader.initFromPage(values.length, writer.getBytes().toInputStream());
    for (int i = 0; i < values.length; i += 2) {
      assertThat(reader.readBytes()).isEqualTo(Binary.fromString(values[i]));
      reader.skip();
    }

    reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(values.length, writer.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < values.length; i += skipCount + 1) {
      skipCount = (values.length - i) / 2;
      assertThat(reader.readBytes()).isEqualTo(Binary.fromString(values[i]));
      reader.skip(skipCount);
    }
  }

  @Test
  public void testLengths() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    Utils.writeData(writer, values);
    int[] bin = Utils.readInts(reader, writer.getBytes().toInputStream(), values.length);

    for (int i = 0; i < bin.length; i++) {
      assertThat(bin[i]).isEqualTo(values[i].length());
    }
  }

  @Test
  public void testNegativeLength() throws IOException {
    DeltaLengthByteArrayValuesReader reader = readerFor("abcd", 2, -2, 2);

    assertThat(reader.readBytes()).isEqualTo(Binary.fromString("ab"));
    assertThatThrownBy(reader::readBytes)
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid DELTA_LENGTH_BYTE_ARRAY value length: -2");
  }

  @Test
  public void testSkipNegativeLength() throws IOException {
    DeltaLengthByteArrayValuesReader reader = readerFor("abcd", 2, -2, 2);

    assertThatThrownBy(() -> reader.skip(3))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Invalid DELTA_LENGTH_BYTE_ARRAY value length: -2");
  }

  @Test
  public void testSkipLengthsOverflowingInt() throws IOException {
    DeltaLengthByteArrayValuesReader reader = readerFor("abcd", Integer.MAX_VALUE, Integer.MAX_VALUE);

    assertThatThrownBy(() -> reader.skip(2))
        .isInstanceOf(ParquetDecodingException.class)
        .hasMessage("Failed to skip 4294967294 bytes");
  }

  private static DeltaLengthByteArrayValuesReader readerFor(String data, int... lengths) throws IOException {
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(lengths.length, pageWithLengths(data, lengths));
    return reader;
  }

  private static ByteBufferInputStream pageWithLengths(String data, int... lengths) throws IOException {
    DeltaBinaryPackingValuesWriterForInteger lengthWriter =
        new DeltaBinaryPackingValuesWriterForInteger(64, 64, new DirectByteBufferAllocator());
    for (int length : lengths) {
      lengthWriter.writeInteger(length);
    }
    byte[] page = BytesInput.concat(lengthWriter.getBytes(), BytesInput.from(data.getBytes(StandardCharsets.UTF_8)))
        .toByteArray();
    return ByteBufferInputStream.wrap(ByteBuffer.wrap(page));
  }
}
