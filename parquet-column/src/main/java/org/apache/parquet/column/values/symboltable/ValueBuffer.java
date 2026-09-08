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
package org.apache.parquet.column.values.symboltable;

import java.util.Arrays;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.api.Binary;

/**
 * The values of one page, held as a single byte array plus start offsets.
 *
 * <p>Training needs to read values in an order of its own choosing, and more than once, so the
 * values have to be somewhere addressable when the page is closed. Holding them as one array rather
 * than as a list of separately allocated values keeps that at one copy of the page with no
 * per-value object, which matters because a writer is already buffering this data anyway.
 *
 * <p>The array carries eight bytes of zero padding past the last value. Symbol matching loads eight
 * bytes at a time and deliberately reads past the end of a short value; the padding is what makes
 * that read in-bounds, and zeros are what make it harmless, since no symbol can match beyond the
 * value's own length once the load is masked.
 */
public class ValueBuffer {

  /** Slack past the last value, for eight-byte loads that overshoot. */
  public static final int TAIL_PADDING = 8;

  private static final int DEFAULT_INITIAL_CAPACITY = 64 * 1024;
  private static final int DEFAULT_INITIAL_VALUES = 1024;

  private byte[] data;
  private int size;
  private int[] offsets;
  private int valueCount;

  public ValueBuffer() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_INITIAL_VALUES);
  }

  public ValueBuffer(int initialCapacity, int initialValues) {
    this.data = new byte[initialCapacity + TAIL_PADDING];
    this.offsets = new int[initialValues + 1];
    this.offsets[0] = 0;
  }

  public void add(Binary value) {
    int length = value.length();
    ensureCapacity(size + length);
    ensureValues(valueCount + 1);
    // Read through a duplicate so the value's own buffer position is left alone. This is the one
    // copy of the value; taking the backing array instead would copy for slice-backed values.
    value.toByteBuffer().duplicate().get(data, size, length);
    size += length;
    offsets[++valueCount] = size;
  }

  public void add(byte[] source, int offset, int length) {
    ensureCapacity(size + length);
    ensureValues(valueCount + 1);
    System.arraycopy(source, offset, data, size, length);
    size += length;
    offsets[++valueCount] = size;
  }

  public int valueCount() {
    return valueCount;
  }

  /** Total length of all values, which is the size of the region {@link #data} holds. */
  public int byteCount() {
    return size;
  }

  /**
   * The backing array. Valid from 0 to {@link #byteCount()}, followed by {@link #TAIL_PADDING} zero
   * bytes that a reader may load but must not interpret.
   */
  public byte[] data() {
    return data;
  }

  public int offset(int index) {
    return offsets[index];
  }

  public int length(int index) {
    return offsets[index + 1] - offsets[index];
  }

  public BytesInput asBytesInput() {
    return BytesInput.from(data, 0, size);
  }

  public void reset() {
    // Clear through the padding so the next round of eight-byte overshoot still reads zeros.
    Arrays.fill(data, 0, Math.min(size + TAIL_PADDING, data.length), (byte) 0);
    size = 0;
    valueCount = 0;
  }

  private void ensureCapacity(int required) {
    if (required + TAIL_PADDING <= data.length) {
      return;
    }
    int capacity = Math.max(data.length * 2, required + TAIL_PADDING);
    data = Arrays.copyOf(data, capacity);
  }

  private void ensureValues(int required) {
    if (required < offsets.length) {
      return;
    }
    offsets = Arrays.copyOf(offsets, Math.max(offsets.length * 2, required + 1));
  }
}
