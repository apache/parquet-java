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

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.column.values.Utils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.io.api.Binary;

public class TestDeltaLengthByteArray {

  String[] values = { "parquet", "hadoop", "mapreduce"};

  private DeltaLengthByteArrayValuesWriter getDeltaLengthByteArrayValuesWriter() {
    return new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024, new DirectByteBufferAllocator());
  }

  @Test
  public void testSerialization () throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();
    
    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
    }
  }

  @Test
  public void testRandomStrings() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    String[] values = Utils.getRandomStringSamples(1000, 32);
    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toInputStream(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
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
      Assert.assertEquals(Binary.fromString(values[i]), reader.readBytes());
      reader.skip();
    }

    reader = new DeltaLengthByteArrayValuesReader();
    reader.initFromPage(values.length, writer.getBytes().toInputStream());
    int skipCount;
    for (int i = 0; i < values.length; i += skipCount + 1) {
      skipCount = (values.length - i) / 2;
      Assert.assertEquals(Binary.fromString(values[i]), reader.readBytes());
      reader.skip(skipCount);
    }
  }

  @Test
  public void testLengths() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = getDeltaLengthByteArrayValuesWriter();
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    Utils.writeData(writer, values);
    int[] bin = Utils.readInts(reader, writer.getBytes().toInputStream(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(values[i].length(), bin[i]);
    }
  }
}
