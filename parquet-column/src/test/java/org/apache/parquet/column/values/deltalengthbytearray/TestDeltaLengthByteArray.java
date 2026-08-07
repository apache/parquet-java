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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
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
  public void testWriteBytesRawArray() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    // Write using the raw byte[] overload with various offsets
    byte[] buf = "XXparquetXXhadoopXXmapreduceXX".getBytes(StandardCharsets.UTF_8);
    writer.writeBytes(buf, 2, 7); // "parquet"
    writer.writeBytes(buf, 11, 6); // "hadoop"
    writer.writeBytes(buf, 19, 9); // "mapreduce"

    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), values.length);

    for (int i = 0; i < bin.length; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
    }
  }

  @Test
  public void testWriteBytesRawArrayEmpty() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    // Write empty byte arrays
    writer.writeBytes(new byte[0], 0, 0);
    writer.writeBytes("hello".getBytes(StandardCharsets.UTF_8), 0, 5);
    writer.writeBytes(new byte[10], 5, 0); // zero-length from middle of array

    reader.initFromPage(3, writer.getBytes().toInputStream());
    Assert.assertEquals(Binary.fromString(""), reader.readBytes());
    Assert.assertEquals(Binary.fromString("hello"), reader.readBytes());
    Assert.assertEquals(Binary.fromString(""), reader.readBytes());
  }

  @Test
  public void testWriteBytesRawArrayMatchesBinaryWrite() throws IOException {
    // Verify that writeBytes(byte[], offset, length) produces identical
    // encoded output to writeBytes(Binary)
    DeltaLengthByteArrayValuesWriter writerBinary = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesWriter writerRaw = getDeltaLengthByteArrayValuesWriter();

    String[] testValues = Utils.getRandomStringSamples(500, 64);
    for (String s : testValues) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      writerBinary.writeBytes(Binary.fromConstantByteArray(bytes));
      writerRaw.writeBytes(bytes, 0, bytes.length);
    }

    Assert.assertArrayEquals(
        writerBinary.getBytes().toByteArray(), writerRaw.getBytes().toByteArray());
  }
}
