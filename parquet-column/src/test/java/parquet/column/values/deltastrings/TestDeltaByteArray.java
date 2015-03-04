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
package parquet.column.values.deltastrings;

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

import parquet.column.values.Utils;
import parquet.column.values.ValuesReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.io.api.Binary;

public class TestDeltaByteArray {

  static String[] values = {"parquet-mr", "parquet", "parquet-format"};
  static String[] randvalues = Utils.getRandomStringSamples(10000, 32);

  @Test
  public void testSerialization () throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
    }
  }

  @Test
  public void testRandomStrings() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);
    DeltaByteArrayReader reader = new DeltaByteArrayReader();

    Utils.writeData(writer, randvalues);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), randvalues.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(randvalues[i]), bin[i]);
    }
  }

  @Test
  public void testLengths() throws IOException {
    DeltaByteArrayWriter writer = new DeltaByteArrayWriter(64 * 1024, 64 * 1024);
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    Utils.writeData(writer, values);
    byte[] data = writer.getBytes().toByteArray();
    int[] bin = Utils.readInts(reader, data, values.length);

    // test prefix lengths
    Assert.assertEquals(0, bin[0]);
    Assert.assertEquals(7, bin[1]);
    Assert.assertEquals(7, bin[2]);

    int offset = reader.getNextOffset();
    reader = new DeltaBinaryPackingValuesReader();
    bin = Utils.readInts(reader, writer.getBytes().toByteArray(), offset, values.length);
    // test suffix lengths
    Assert.assertEquals(10, bin[0]);
    Assert.assertEquals(0, bin[1]);
    Assert.assertEquals(7, bin[2]);
  }
}
