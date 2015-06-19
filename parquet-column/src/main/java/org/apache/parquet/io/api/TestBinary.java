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
package org.apache.parquet.io.api;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class TestBinary {
  @Test
  public void testSliceForByteArrayBackedBinary() {
    Binary binary = Binary.fromConstantByteArray("test-123".getBytes());
    Assert.assertArrayEquals("123".getBytes(), binary.slice(5, 3).getBytesUnsafe());
  }

  @Test
  public void testSliceForByteArraySliceBackedBinary() {
    Binary binary = Binary.fromConstantByteArray("test-slice-123".getBytes(), 5, 9);
    Assert.assertArrayEquals("123".getBytes(), binary.slice(6, 3).getBytesUnsafe());
  }

  @Test
  public void testSliceForByteBufferBackedBinary() {
    Binary binary = Binary.fromConstantByteBuffer(ByteBuffer.wrap("test-123".getBytes()));
    Assert.assertArrayEquals("123".getBytes(), binary.slice(5, 3).getBytesUnsafe());
  }

  @Test
  public void testCopyForConstantByteArrayBackedBinary() {
    byte[] bytes = "test-123".getBytes();
    Binary binary = Binary.fromConstantByteArray(bytes);
    Assert.assertArrayEquals("test-123".getBytes(), binary.copy().getBytesUnsafe());

    final Binary copy = binary.copy();
    bytes[4] = '.';
    Assert.assertArrayEquals("test.123".getBytes(), copy.getBytesUnsafe());
  }

  @Test
  public void testCopyForReusedByteArrayBackedBinary() {
    byte[] bytes = "test-123".getBytes();
    Binary binary = Binary.fromReusedByteArray(bytes);
    Assert.assertArrayEquals("test-123".getBytes(), binary.copy().getBytesUnsafe());

    final Binary copy = binary.copy();
    bytes[4] = '.';
    Assert.assertArrayEquals("test-123".getBytes(), copy.getBytesUnsafe());
  }

  @Test
  public void testCopyForConstantByteArraySliceBackedBinary() {
    byte[] bytes = "slice-test-123".getBytes();
    Binary binary = Binary.fromConstantByteArray(bytes, 6, 8);
    Assert.assertArrayEquals("test-123".getBytes(), binary.copy().getBytesUnsafe());

    final Binary copy = binary.copy();
    bytes[10] = '.';
    Assert.assertArrayEquals("test.123".getBytes(), copy.getBytesUnsafe());
  }

  @Test
  public void testCopyForReusedByteArraySliceBackedBinary() {
    byte[] bytes = "slice-test-123".getBytes();
    Binary binary = Binary.fromReusedByteArray(bytes, 6, 8);
    Assert.assertArrayEquals("test-123".getBytes(), binary.copy().getBytesUnsafe());

    final Binary copy = binary.copy();
    bytes[10] = '.';
    Assert.assertArrayEquals("test-123".getBytes(), copy.getBytesUnsafe());
  }

  @Test
  public void testCopyForConstantByteBufferBackedBinary() {
    byte[] bytes = "test-123".getBytes();
    Binary binary = Binary.fromConstantByteBuffer(ByteBuffer.wrap(bytes));
    Assert.assertArrayEquals("test-123".getBytes(), binary.copy().getBytesUnsafe());

    final Binary copy = binary.copy();
    bytes[4] = '.'; //ByteBuffer's copy always does a copy, unlike other implementations
    Assert.assertArrayEquals("test-123".getBytes(), copy.getBytesUnsafe());
  }

  @Test
  public void testCopyForReusedByteBufferBackedBinary() {
    byte[] bytes = "test-123".getBytes();
    Binary binary = Binary.fromReusedByteBuffer(ByteBuffer.wrap(bytes));
    Assert.assertArrayEquals("test-123".getBytes(), binary.copy().getBytesUnsafe());

    final Binary copy = binary.copy();
    bytes[4] = '.';
    Assert.assertArrayEquals("test-123".getBytes(), copy.getBytesUnsafe());
  }
}
