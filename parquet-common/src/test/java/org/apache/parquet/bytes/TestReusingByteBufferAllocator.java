/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.bytes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.util.Random;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestReusingByteBufferAllocator {

  private static final Random RANDOM = new Random(2024_02_22_09_51L);

  private TrackingByteBufferAllocator allocator;

  @Parameter
  public ByteBufferAllocator innerAllocator;

  @Parameters(name = "{0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        new HeapByteBufferAllocator() {
          @Override
          public String toString() {
            return "HEAP";
          }
        }
      },
      {
        new DirectByteBufferAllocator() {
          @Override
          public String toString() {
            return "DIRECT";
          }
        }
      }
    };
  }

  @Before
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(innerAllocator);
  }

  @After
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void normalUseCase() {
    try (ReusingByteBufferAllocator reusingAllocator = new ReusingByteBufferAllocator(allocator)) {
      assertEquals(innerAllocator.isDirect(), reusingAllocator.isDirect());
      for (int i = 0; i < 10; ++i) {
        try (ByteBufferReleaser releaser = reusingAllocator.getReleaser()) {
          int size = RANDOM.nextInt(1024);
          ByteBuffer buf = reusingAllocator.allocate(size);
          releaser.releaseLater(buf);

          assertEquals(0, buf.position());
          assertEquals(size, buf.capacity());
          assertEquals(size, buf.remaining());
          assertEquals(allocator.isDirect(), buf.isDirect());
          assertThrows(InvalidMarkException.class, buf::reset);

          // Let's see if the next allocate would clear the buffer
          buf.position(buf.capacity() / 2);
          buf.mark();
          buf.position(buf.limit());
        }
      }

      // Check if actually releasing the buffer is independent of the release call in the reusing allocator
      reusingAllocator.allocate(1025);
    }
  }

  @Test
  public void validateExceptions() {
    try (ByteBufferReleaser releaser = new ByteBufferReleaser(allocator);
        ReusingByteBufferAllocator reusingAllocator = new ReusingByteBufferAllocator(allocator)) {
      ByteBuffer fromOther = allocator.allocate(10);
      releaser.releaseLater(fromOther);

      assertThrows(IllegalStateException.class, () -> reusingAllocator.release(fromOther));

      ByteBuffer fromReusing = reusingAllocator.allocate(10);

      assertThrows(IllegalArgumentException.class, () -> reusingAllocator.release(fromOther));
      assertThrows(IllegalStateException.class, () -> reusingAllocator.allocate(10));

      reusingAllocator.release(fromReusing);
      assertThrows(IllegalStateException.class, () -> reusingAllocator.release(fromOther));
      assertThrows(IllegalStateException.class, () -> reusingAllocator.release(fromReusing));
    }
  }
}
