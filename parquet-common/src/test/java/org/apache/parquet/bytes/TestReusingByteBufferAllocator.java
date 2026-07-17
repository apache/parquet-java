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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestReusingByteBufferAllocator {

  private enum AllocatorType {
    STRICT(ReusingByteBufferAllocator::strict),
    UNSAFE(ReusingByteBufferAllocator::unsafe);
    private final Function<ByteBufferAllocator, ReusingByteBufferAllocator> factory;

    AllocatorType(Function<ByteBufferAllocator, ReusingByteBufferAllocator> factory) {
      this.factory = factory;
    }

    public ReusingByteBufferAllocator create(ByteBufferAllocator allocator) {
      return factory.apply(allocator);
    }
  }

  private static final Random RANDOM = new Random(2024_02_22_09_51L);

  private TrackingByteBufferAllocator allocator;

  @Parameter
  public ByteBufferAllocator innerAllocator;

  @Parameter(1)
  public AllocatorType type;

  @Parameters(name = "{0} {1}")
  public static List<Object[]> parameters() {
    List<Object[]> params = new ArrayList<>();
    for (Object allocator : new Object[] {
      new HeapByteBufferAllocator() {
        @Override
        public String toString() {
          return "HEAP";
        }
      },
      new DirectByteBufferAllocator() {
        @Override
        public String toString() {
          return "DIRECT";
        }
      }
    }) {
      for (Object type : AllocatorType.values()) {
        params.add(new Object[] {allocator, type});
      }
    }
    return params;
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
    try (ReusingByteBufferAllocator reusingAllocator = type.create(allocator)) {
      assertThat(reusingAllocator.isDirect()).isEqualTo(innerAllocator.isDirect());
      for (int i = 0; i < 10; ++i) {
        try (ByteBufferReleaser releaser = reusingAllocator.getReleaser()) {
          int size = RANDOM.nextInt(1024);
          ByteBuffer buf = reusingAllocator.allocate(size);
          releaser.releaseLater(buf);

          validateBuffer(buf, size);

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

  private void validateBuffer(ByteBuffer buf, int size) {
    assertThat(buf.position()).isEqualTo(0);
    assertThat(buf.capacity()).isEqualTo(size);
    assertThat(buf.remaining()).isEqualTo(size);
    assertThat(buf.isDirect()).isEqualTo(allocator.isDirect());
    assertThatThrownBy(buf::reset).isInstanceOf(InvalidMarkException.class);
  }

  @Test
  public void validateExceptions() {
    try (ByteBufferReleaser releaser = new ByteBufferReleaser(allocator);
        ReusingByteBufferAllocator reusingAllocator = type.create(allocator)) {
      ByteBuffer fromOther = allocator.allocate(10);
      releaser.releaseLater(fromOther);

      assertThatThrownBy(() -> reusingAllocator.release(fromOther))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("The single buffer has already been released or never allocated");

      ByteBuffer fromReusing = reusingAllocator.allocate(10);

      assertThatThrownBy(() -> reusingAllocator.release(fromOther))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("The buffer to be released is not the one allocated by this allocator");
      switch (type) {
        case STRICT:
          assertThatThrownBy(() -> reusingAllocator.allocate(5))
              .isInstanceOf(IllegalStateException.class)
              .hasMessage("The single buffer is not yet released");
          break;
        case UNSAFE:
          fromReusing = reusingAllocator.allocate(5);
          validateBuffer(fromReusing, 5);
          break;
      }

      reusingAllocator.release(fromReusing);
      ByteBuffer fromReusingFinal = fromReusing;
      assertThatThrownBy(() -> reusingAllocator.release(fromOther))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("The single buffer has already been released or never allocated");
      assertThatThrownBy(() -> reusingAllocator.release(fromReusingFinal))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("The single buffer has already been released or never allocated");
    }
  }
}
