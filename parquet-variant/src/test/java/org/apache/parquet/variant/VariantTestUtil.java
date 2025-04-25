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
package org.apache.parquet.variant;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariantTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(VariantTestUtil.class);
  private static final String RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

  /** Random number generator for generating random strings */
  private static SecureRandom random = new SecureRandom(new byte[] {1, 2, 3, 4, 5});

  static final ByteBuffer EMPTY_METADATA = ByteBuffer.wrap(new byte[] {0b1});

  static void checkType(Variant v, int expectedBasicType, Variant.Type expectedType) {
    Assert.assertEquals(expectedBasicType, v.value.get(v.value.position()) & VariantUtil.BASIC_TYPE_MASK);
    Assert.assertEquals(expectedType, v.getType());
  }

  static long microsSinceEpoch(Instant instant) {
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + instant.getNano() / 1000;
  }

  static long nanosSinceEpoch(Instant instant) {
    return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
  }

  static String randomString(int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(RANDOM_CHARS.charAt(random.nextInt(RANDOM_CHARS.length())));
    }
    return sb.toString();
  }

  static void testVariant(Variant v, Consumer<Variant> consumer) {
    consumer.accept(v);
    // Create new Variant with different byte offsets
    byte[] newValue = new byte[v.value.capacity() + 50];
    byte[] newMetadata = new byte[v.metadata.capacity() + 50];
    Arrays.fill(newValue, (byte) 0xFF);
    Arrays.fill(newMetadata, (byte) 0xFF);
    v.value.position(0);
    v.value.get(newValue, 25, v.value.capacity());
    v.value.position(0);
    v.metadata.position(0);
    v.metadata.get(newMetadata, 25, v.metadata.capacity());
    v.metadata.position(0);
    Variant v2 = new Variant(
        ByteBuffer.wrap(newValue, 25, v.value.capacity()),
        ByteBuffer.wrap(newMetadata, 25, v.metadata.capacity()));
    consumer.accept(v2);
  }

  static byte primitiveHeader(int type) {
    return (byte) (type << 2);
  }

  static byte metadataHeader(boolean isSorted, int offsetSize) {
    return (byte) (((offsetSize - 1) << 6) | (isSorted ? 0b10000 : 0) | 0b0001);
  }

  static byte[] constructString(String value) {
    return ByteBuffer.allocate(value.length() + 5)
        .order(ByteOrder.LITTLE_ENDIAN)
        .put(primitiveHeader(16))
        .putInt(value.length())
        .put(value.getBytes(StandardCharsets.UTF_8))
        .array();
  }

  static int getMinIntegerSize(int value) {
    return (value <= 0xFF) ? 1 : (value <= 0xFFFF) ? 2 : (value <= 0xFFFFFF) ? 3 : 4;
  }

  static void writeVarlenInt(ByteBuffer buffer, int value, int valueSize) {
    if (valueSize == 1) {
      buffer.put((byte) value);
    } else if (valueSize == 2) {
      buffer.putShort((short) value);
    } else if (valueSize == 3) {
      buffer.put((byte) (value & 0xFF));
      buffer.put((byte) ((value >> 8) & 0xFF));
      buffer.put((byte) ((value >> 16) & 0xFF));
    } else {
      buffer.putInt(value);
    }
  }
}
