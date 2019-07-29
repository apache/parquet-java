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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.junit.Test;
import org.junit.Assert;

import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.api.Binary;

public class TestDeltaByteArray {

  static String[] values = {"parquet-mr", "parquet", "parquet-format"};
  static String[] randvalues = Utils.getRandomStringSamples(10000, 32);

  @Test
  public void testSerialization () throws Exception {
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
    Assert.assertEquals(0, bin[0]);
    Assert.assertEquals(7, bin[1]);
    Assert.assertEquals(7, bin[2]);

    reader = new DeltaBinaryPackingValuesReader();
    bin = Utils.readInts(reader, data, values.length);
    // test suffix lengths
    Assert.assertEquals(10, bin[0]);
    Assert.assertEquals(0, bin[1]);
    Assert.assertEquals(7, bin[2]);
  }

  private void assertReadWrite(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals) throws Exception {
    Utils.writeData(writer, vals);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), vals.length);

    for(int i = 0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(vals[i]), bin[i]);
    }
  }

  private void assertReadWriteWithSkip(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals) throws Exception {
    Utils.writeData(writer, vals);

    reader.initFromPage(vals.length, writer.getBytes().toInputStream());
    for (int i = 0; i < vals.length; i += 2) {
      Assert.assertEquals(Binary.fromString(vals[i]), reader.readBytes());
      reader.skip();
    }
  }

  private void assertReadWriteWithSkipN(DeltaByteArrayWriter writer, DeltaByteArrayReader reader, String[] vals) throws Exception {
    Utils.writeData(writer, vals);

    reader.initFromPage(vals.length, writer.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < vals.length; i += skipCount + 1) {
      skipCount = (vals.length - i) / 2;
      Assert.assertEquals(Binary.fromString(vals[i]), reader.readBytes());
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
}
