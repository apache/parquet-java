/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.deltastrings;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesWriter;
import parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import parquet.io.api.Binary;

public class TestDeltaStrings {
  
  String[] values = {"parquet-mr", "parquet", "parquet-format"};

  @Test
  public void testSerialization () throws IOException {
    DeltaStringValuesWriter writer = new DeltaStringValuesWriter(64*1024);
    DeltaStringValuesReader reader = new DeltaStringValuesReader();

    writeData(writer, values);
    Binary[] bin = readData(reader, writer.getBytes().toByteArray(), values.length);

    for(int i =0; i< bin.length ; i++) {
      Assert.assertEquals(Binary.fromString(values[i]), bin[i]);
    }
  }

  @Test
  public void testLengths() throws IOException {
    DeltaStringValuesWriter writer = new DeltaStringValuesWriter(64*1024);
    ValuesReader reader = new DeltaBinaryPackingValuesReader();

    writeData(writer, values);
    byte[] data = writer.getBytes().toByteArray();
    int[] bin = readInts(reader, data, 0, values.length);

    // test prefix lengths
    Assert.assertEquals(0, bin[0]);
    Assert.assertEquals(7, bin[1]);
    Assert.assertEquals(7, bin[2]);
    
    int offset = reader.getNextOffset();
    reader = new DeltaBinaryPackingValuesReader();
    bin = readInts(reader, writer.getBytes().toByteArray(), offset, values.length);
    // test suffix lengths
    Assert.assertEquals(10, bin[0]);
    Assert.assertEquals(0, bin[1]);
    Assert.assertEquals(7, bin[2]);
  }

  private void writeData(ValuesWriter writer, String[] strings)
      throws IOException {
    for(int i=0; i < strings.length; i++) {
      writer.writeBytes(Binary.fromString(strings[i]));
    }
  }

  private static Binary[] readData(ValuesReader reader, byte[] data, int length)
      throws IOException {
    Binary[] bins = new Binary[length];
    reader.initFromPage(length, data, 0);
    for(int i=0; i < length; i++) {
      bins[i] = reader.readBytes();
    }
    return bins;
  }
  
  private static int[] readInts(ValuesReader reader, byte[] data, int offset, int length)
      throws IOException {
    int[] ints = new int[length];
    reader.initFromPage(length, data, offset);
    for(int i=0; i < length; i++) {
      ints[i] = reader.readInteger();
    }
    return ints;
  }
}
