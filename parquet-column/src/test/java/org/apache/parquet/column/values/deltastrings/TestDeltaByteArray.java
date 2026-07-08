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
package org.apache.parquet.column.values.deltastrings;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestDeltaByteArray {

  static String[] values = {"parquet-mr", "parquet", "parquet-format"};
  static String[] randvalues = Utils.getRandomStringSamples(10000, 32);

  @Test
  public void testSerialization() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    assertReadWrite(writer, reader, values);
  }

  @Test
  public void testRandomStrings() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    assertReadWrite(writer, reader, randvalues);
  }

  @Test
  public void testRandomStringsWithSkip() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    assertReadWriteWithSkip(writer, reader, randvalues);
  }

  @Test
  public void testRandomStringsWithSkipN() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();
    assertReadWriteWithSkipN(writer, reader, randvalues);
  }

  @Test
  public void testLengths() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    Utils.writeData(writer, values);
    ByteBufferInputStream data = writer.getBytes().toInputStream();
    int[] bin = Utils.readInts(reader, data, values.length);

    // test prefix lengths
    assertThat(bin[0]).isEqualTo(0);
    assertThat(bin[1]).isEqualTo(7);
    assertThat(bin[2]).isEqualTo(7);

    reader = new DeltaBinaryPackingValuesReader();
    bin = Utils.readInts(reader, data, values.length);
    // test suffix lengths
    assertThat(bin[0]).isEqualTo(10);
    assertThat(bin[1]).isEqualTo(0);
    assertThat(bin[2]).isEqualTo(7);
  }

  private void assertReadWrite(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals)
      throws Exception {
    Utils.writeData(writer, vals);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), vals.length);

    for (int i = 0; i < bin.length; i++) {
      assertThat(bin[i]).isEqualTo(Binary.fromString(vals[i]));
    }
  }

  private void assertReadWriteWithSkip(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals)
      throws Exception {
    Utils.writeData(writer, vals);

    reader.initFromPage(vals.length, writer.getBytes().toInputStream());
    for (int i = 0; i < vals.length; i += 2) {
      assertThat(reader.readBytes()).isEqualTo(Binary.fromString(vals[i]));
      reader.skip();
    }
  }

  private void assertReadWriteWithSkipN(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals)
      throws Exception {
    Utils.writeData(writer, vals);

    reader.initFromPage(vals.length, writer.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < vals.length; i += skipCount + 1) {
      skipCount = (vals.length - i) / 2;
      assertThat(reader.readBytes()).isEqualTo(Binary.fromString(vals[i]));
      reader.skip(skipCount);
    }
  }

  @Test
  public void testWriterReset() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());

    assertReadWrite(writer, new DeltaByteArrayReader(), values);

    writer.reset();

    assertReadWrite(writer, new DeltaByteArrayReader(), values);
  }

  @Test
  public void testReusedBackingArrayRegression() throws Exception {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    byte[] buffer = "parquet-000".getBytes(StandardCharsets.UTF_8);
    writer.writeBytes(Binary.fromReusedByteArray(buffer));

    System.arraycopy("parquet-111".getBytes(StandardCharsets.UTF_8), 0, buffer, 0, buffer.length);
    writer.writeBytes(Binary.fromReusedByteArray(buffer));

    System.arraycopy("parquet-222".getBytes(StandardCharsets.UTF_8), 0, buffer, 0, buffer.length);
    writer.writeBytes(Binary.fromReusedByteArray(buffer));

    Binary[] decoded = Utils.readData(reader, writer.getBytes().toInputStream(), 3);
    assertThat(decoded[0]).isEqualTo(Binary.fromString("parquet-000"));
    assertThat(decoded[1]).isEqualTo(Binary.fromString("parquet-111"));
    assertThat(decoded[2]).isEqualTo(Binary.fromString("parquet-222"));
  }
}
