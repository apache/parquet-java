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
package org.apache.parquet.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class of {@link ConcatenatingByteBufferCollector}.
 */
public class TestConcatenatingByteBufferCollector {

  private TrackingByteBufferAllocator allocator;

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void test() throws IOException {
    byte[] result;
    try (ConcatenatingByteBufferCollector outer = new ConcatenatingByteBufferCollector(allocator);
        ConcatenatingByteBufferCollector inner = new ConcatenatingByteBufferCollector(allocator)) {
      outer.collect(BytesInput.concat(
          BytesInput.from(byteBuffer("This"), byteBuffer(" "), byteBuffer("is")),
          BytesInput.from(Arrays.asList(byteBuffer(" a"), byteBuffer(" "), byteBuffer("test"))),
          BytesInput.from(inputStream(" text to blabla"), 8),
          BytesInput.from(bytes(" ")),
          BytesInput.from(bytes("blabla validate blabla"), 7, 9),
          BytesInput.from(byteArrayOutputStream("the class ")),
          BytesInput.from(capacityByteArrayOutputStream("ConcatenatingByteBufferCollector"))));
      inner.collect(BytesInput.fromInt(12345));
      inner.collect(BytesInput.fromUnsignedVarInt(67891));
      inner.collect(BytesInput.fromUnsignedVarLong(2345678901L));
      inner.collect(BytesInput.fromZigZagVarInt(-234567));
      inner.collect(BytesInput.fromZigZagVarLong(-890123456789L));
      outer.collect(inner);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      outer.writeAllTo(baos);
      result = baos.toByteArray();
    }

    Assert.assertEquals(
        "This is a test text to validate the class ConcatenatingByteBufferCollector",
        new String(result, 0, 74));
    InputStream in = new ByteArrayInputStream(result, 74, result.length - 74);
    Assert.assertEquals(12345, BytesUtils.readIntLittleEndian(in));
    Assert.assertEquals(67891, BytesUtils.readUnsignedVarInt(in));
    Assert.assertEquals(2345678901L, BytesUtils.readUnsignedVarLong(in));
    Assert.assertEquals(-234567, BytesUtils.readZigZagVarInt(in));
    Assert.assertEquals(-890123456789L, BytesUtils.readZigZagVarLong(in));
  }

  private static byte[] bytes(String str) {
    return str.getBytes(StandardCharsets.UTF_8);
  }

  private static ByteBuffer byteBuffer(String str) {
    return ByteBuffer.wrap(bytes(str));
  }

  private static InputStream inputStream(String str) {
    return new ByteArrayInputStream(bytes(str));
  }

  private static ByteArrayOutputStream byteArrayOutputStream(String str) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(bytes(str));
    return baos;
  }

  private static CapacityByteArrayOutputStream capacityByteArrayOutputStream(String str) {
    CapacityByteArrayOutputStream cbaos =
        new CapacityByteArrayOutputStream(2, Integer.MAX_VALUE, new HeapByteBufferAllocator());
    for (byte b : bytes(str)) {
      cbaos.write(b);
    }
    return cbaos;
  }
}
