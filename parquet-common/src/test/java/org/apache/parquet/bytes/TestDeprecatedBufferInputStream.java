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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests the deprecated behavior of instantiating ByteBufferInputStream directly
 */
@RunWith(Parameterized.class)
public class TestDeprecatedBufferInputStream extends TestByteBufferInputStreams {
  @Parameter(0)
  public ByteBuffer data;
  @Parameter(1)
  public Integer offset;

  @Parameters
  public static List<Object[]> parameters() {
    return Arrays.asList(
        new Object[] { TestSingleBufferInputStream.DATA, null },
        new Object[] { TestSingleBufferInputStream.DATA, 0 },
        new Object[] { ByteBuffer.wrap(new byte[] { -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14,
            15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34 }), 4 },
        new Object[] { ByteBuffer.wrap(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
            19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, -1, -1, -1 }), 0 },
        new Object[] { ByteBuffer.wrap(new byte[] { -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, -1, -1 }), 3 });
  }

  @Override
  protected ByteBufferInputStream newStream() {
    if (offset == null) {
      return new ByteBufferInputStream(data);
    } else {
      return new ByteBufferInputStream(data, offset, DATA_LENGTH);
    }
  }

  @Override
  protected void checkOriginalData() {
    Assert.assertEquals("Position should not change", 0, data.position());
    Assert.assertEquals("Limit should not change",
        data.array().length, data.limit());
  }

  @Test
  public void testSliceData() throws Exception {
    ByteBufferInputStream stream = newStream();
    int length = stream.available();

    List<ByteBuffer> buffers = new ArrayList<>();
    // slice the stream into 3 8-byte buffers and 1 2-byte buffer
    while (stream.available() > 0) {
      int bytesToSlice = Math.min(stream.available(), 8);
      buffers.add(stream.slice(bytesToSlice));
    }

    Assert.assertEquals("Position should be at end", length, stream.position());
    Assert.assertEquals("Should produce 5 buffers", 5, buffers.size());

    int i = 0;

    ByteBuffer one = buffers.get(0);
    Assert.assertSame("Should use the same backing array",
        one.array(), data.array());
    Assert.assertEquals(8, one.remaining());
    Assert.assertEquals(0, one.position());
    Assert.assertEquals(8, one.limit());
    for (; i < 8; i += 1) {
      Assert.assertEquals("Should produce correct values", i, one.get());
    }

    ByteBuffer two = buffers.get(1);
    Assert.assertSame("Should use the same backing array",
        two.array(), data.array());
    Assert.assertEquals(8, two.remaining());
    Assert.assertEquals(8, two.position());
    Assert.assertEquals(16, two.limit());
    for (; i < 16; i += 1) {
      Assert.assertEquals("Should produce correct values", i, two.get());
    }

    // three is a copy of part of the 4th buffer
    ByteBuffer three = buffers.get(2);
    Assert.assertSame("Should use the same backing array",
        three.array(), data.array());
    Assert.assertEquals(8, three.remaining());
    Assert.assertEquals(16, three.position());
    Assert.assertEquals(24, three.limit());
    for (; i < 24; i += 1) {
      Assert.assertEquals("Should produce correct values", i, three.get());
    }

    // four should be a copy of the next 8 bytes
    ByteBuffer four = buffers.get(3);
    Assert.assertSame("Should use the same backing array",
        four.array(), data.array());
    Assert.assertEquals(8, four.remaining());
    Assert.assertEquals(24, four.position());
    Assert.assertEquals(32, four.limit());
    for (; i < 32; i += 1) {
      Assert.assertEquals("Should produce correct values", i, four.get());
    }

    // five should be a copy of the next 8 bytes
    ByteBuffer five = buffers.get(4);
    Assert.assertSame("Should use the same backing array",
        five.array(), data.array());
    Assert.assertEquals(3, five.remaining());
    Assert.assertEquals(32, five.position());
    Assert.assertEquals(35, five.limit());
    for (; i < 35; i += 1) {
      Assert.assertEquals("Should produce correct values", i, five.get());
    }
  }

  @Test
  public void testWholeSliceBuffersData() throws Exception {
    ByteBufferInputStream stream = newStream();

    List<ByteBuffer> buffers = stream.sliceBuffers(stream.available());
    Assert.assertEquals("Should return duplicates of all non-empty buffers",
        Collections.singletonList(TestSingleBufferInputStream.DATA), buffers);
  }
}
