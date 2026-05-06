/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.variant;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Metadata that adds keys as needed.
 */
public class MetadataBuilder implements Metadata {

  /** The dictionary for mapping keys to monotonically increasing ids. */
  private HashMap<String, Integer> dictionary = new HashMap<>();
  /** The keys in the dictionary, in id order. */
  private ArrayList<byte[]> dictionaryKeys = new ArrayList<>();

  @Override
  public ByteBuffer getEncodedBuffer() {
    int numKeys = dictionaryKeys.size();
    // Use long to avoid overflow in accumulating lengths.
    long dictionaryTotalDataSize = 0;
    for (byte[] key : dictionaryKeys) {
      dictionaryTotalDataSize += key.length;
    }
    // Determine the number of bytes required per offset entry.
    // The largest offset is the one-past-the-end value, which is total data size. It's very
    // unlikely that the number of keys could be larger, but incorporate that into the calculation
    // in case of pathological data.
    long maxSize = Math.max(dictionaryTotalDataSize, numKeys);
    int offsetSize = VariantBuilder.getMinIntegerSize((int) maxSize);

    int offsetListOffset = 1 + offsetSize;
    int dataOffset = offsetListOffset + (numKeys + 1) * offsetSize;
    long metadataSize = dataOffset + dictionaryTotalDataSize;

    byte[] metadata = new byte[(int) metadataSize];
    // Only unsorted dictionary keys are supported.
    // TODO: Support sorted dictionary keys.
    int headerByte = VariantUtil.VERSION | ((offsetSize - 1) << 6);
    VariantUtil.writeLong(metadata, 0, headerByte, 1);
    VariantUtil.writeLong(metadata, 1, numKeys, offsetSize);
    int currentOffset = 0;
    for (int i = 0; i < numKeys; ++i) {
      VariantUtil.writeLong(metadata, offsetListOffset + i * offsetSize, currentOffset, offsetSize);
      byte[] key = dictionaryKeys.get(i);
      System.arraycopy(key, 0, metadata, dataOffset + currentOffset, key.length);
      currentOffset += key.length;
    }
    VariantUtil.writeLong(metadata, offsetListOffset + numKeys * offsetSize, currentOffset, offsetSize);
    return ByteBuffer.wrap(metadata);
  }

  @Override
  public int getOrInsert(String key) {
    return dictionary.computeIfAbsent(key, newKey -> {
      int id = dictionaryKeys.size();
      dictionaryKeys.add(newKey.getBytes(StandardCharsets.UTF_8));
      return id;
    });
  }
}
