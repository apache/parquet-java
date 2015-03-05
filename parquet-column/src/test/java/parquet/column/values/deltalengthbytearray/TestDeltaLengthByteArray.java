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
package parquet.column.values.deltalengthbytearray;

import java.io.IOException;

import org.junit.Test;
import org.junit.Assert;

import parquet.column.values.Utils;
import parquet.column.values.ValuesReader;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.io.api.Binary;

public class TestDeltaLengthByteArray {

  String[] values = { "parquet", "hadoop", "mapreduce"};

  @Test
  public void testSerialization () throws IOException {
    DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024);
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
    }
  }

  @Test
  public void testRandomStrings() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024);
    DeltaLengthByteArrayValuesReader reader = new DeltaLengthByteArrayValuesReader();

    String[] values = Utils.getRandomStringSamples(1000, 32);
    Utils.writeData(writer, values);
    Binary[] bin = Utils.readData(reader, writer.getBytes().toByteArray(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
    }
  }

  @Test
  public void testLengths() throws IOException {
    DeltaLengthByteArrayValuesWriter writer = new DeltaLengthByteArrayValuesWriter(64 * 1024, 64 * 1024);
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    Utils.writeData(writer, values);
    int[] bin = Utils.readInts(reader, writer.getBytes().toByteArray(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(values[i].length(), bin[i]);
    }
  }
}
