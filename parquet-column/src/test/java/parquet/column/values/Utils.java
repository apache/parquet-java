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
package parquet.column.values;

import java.io.IOException;
import java.util.Random;

import parquet.io.api.Binary;

/**
 * Test Utility class
 * 
 * @author Aniket Mokashi
 *
 */
public class Utils {
  private static Random randomLen = new Random();
  private static RandomStr randomStr = new RandomStr(randomLen);
  
  public static String[] getRandomStringSamples(int numSamples, int maxLength) {
    String[] samples = new String[numSamples];
    
    for (int i=0; i < numSamples; i++) {
      int len = randomLen.nextInt(maxLength);
      samples[i] = randomStr.get(len);
    }
    
    return samples;
  }
  
  public static void writeInts(ValuesWriter writer, int[] ints)
      throws IOException {
    for(int i=0; i < ints.length; i++) {
      writer.writeInteger(ints[i]);
    }
  }

  public static void writeData(ValuesWriter writer, String[] strings)
      throws IOException {
    for(int i=0; i < strings.length; i++) {
      writer.writeBytes(Binary.fromString(strings[i]));
    }
  }

  public static Binary[] readData(ValuesReader reader, byte[] data, int offset, int length)
      throws IOException {
    Binary[] bins = new Binary[length];
    reader.initFromPage(length, data, 0);
    for(int i=0; i < length; i++) {
      bins[i] = reader.readBytes();
    }
    return bins;
  }
  
  public static Binary[] readData(ValuesReader reader, byte[] data, int length)
      throws IOException {
    return readData(reader, data, 0, length);
  }
  
  public static int[] readInts(ValuesReader reader, byte[] data, int offset, int length)
      throws IOException {
    int[] ints = new int[length];
    reader.initFromPage(length, data, offset);
    for(int i=0; i < length; i++) {
      ints[i] = reader.readInteger();
    }
    return ints;
  }
  
  public static int[] readInts(ValuesReader reader, byte[] data, int length)
      throws IOException {
    return readInts(reader, data, 0, length);
  }
}
