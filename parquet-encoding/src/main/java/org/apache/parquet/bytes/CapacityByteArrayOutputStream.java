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

import static java.lang.Math.max;
import static java.lang.Math.pow;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static org.apache.parquet.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.Log;

/**
 * Similar to a {@link ByteArrayOutputStream}, but uses a different strategy for growing that does not involve copying.
 * Where ByteArrayOutputStream is backed by a single array that "grows" by copying into a new larger array, this output
 * stream grows by allocating a new array (slab) and adding it to a list of previous arrays.
 *
 * Each new slab is allocated to be the same size as all the previous slabs combined, so these allocations become
 * exponentially less frequent, just like ByteArrayOutputStream, with one difference. This output stream accepts a
 * max capacity hint, which is a hint describing the max amount of data that will be written to this stream. As the
 * total size of this stream nears this max, this stream starts to grow linearly instead of exponentially.
 * So new slabs are allocated to be 1/5th of the max capacity hint,
 * instead of equal to the total previous size of all slabs. This is useful because it prevents allocating roughly
 * twice the needed space when a new slab is added just before the stream is done being used.
 *
 * When reusing this stream it will adjust the initial slab size based on the previous data size, aiming for fewer
 * allocations, with the assumption that a similar amount of data will be written to this stream on re-use.
 * See ({@link CapacityByteArrayOutputStream#reset()}).
 *
 * @author Julien Le Dem
 *
 */
public class CapacityByteArrayOutputStream extends OutputStream {
  private static final Log LOG = Log.getLog(CapacityByteArrayOutputStream.class);
  private static final byte[] EMPTY_SLAB = new byte[0];

  private int initialSlabSize;
  private final int maxCapacityHint;
  private final List<byte[]> slabs = new ArrayList<byte[]>();

  private byte[] currentSlab;
  private int currentSlabIndex;
  private int bytesAllocated = 0;
  private int bytesUsed = 0;

  /**
   * Return an initial slab size such that a CapacityByteArrayOutputStream constructed with it
   * will end up allocating targetNumSlabs in order to reach targetCapacity. This aims to be
   * a balance between the overhead of creating new slabs and wasting memory by eagerly making
   * initial slabs too big.
   *
   * Note that targetCapacity here need not match maxCapacityHint in the constructor of
   * CapacityByteArrayOutputStream, though often that would make sense.
   *
   * @param minSlabSize no matter what we shouldn't make slabs any smaller than this
   * @param targetCapacity after we've allocated targetNumSlabs how much capacity should we have?
   * @param targetNumSlabs how many slabs should it take to reach targetCapacity?
   */
  public static int initialSlabSizeHeuristic(int minSlabSize, int targetCapacity, int targetNumSlabs) {
    // initialSlabSize = (targetCapacity / (2^targetNumSlabs)) means we double targetNumSlabs times
    // before reaching the targetCapacity
    // eg for page size of 1MB we start at 1024 bytes.
    // we also don't want to start too small, so we also apply a minimum.
    return max(minSlabSize, ((int) (targetCapacity / pow(2, targetNumSlabs))));
  }

  /**
   * Construct a CapacityByteArrayOutputStream configured such that its initial slab size is
   * determined by {@link #initialSlabSizeHeuristic}, with targetCapacity == maxCapacityHint
   */
  public static CapacityByteArrayOutputStream withTargetNumSlabs(
      int minSlabSize, int maxCapacityHint, int targetNumSlabs) {

    return new CapacityByteArrayOutputStream(
        initialSlabSizeHeuristic(minSlabSize, maxCapacityHint, targetNumSlabs),
        maxCapacityHint);
  }

  /**
   * Defaults maxCapacityHint to 1MB
   * @param initialSlabSize
   * @deprecated use {@link CapacityByteArrayOutputStream#CapacityByteArrayOutputStream(int, int)}
   */
  @Deprecated
  public CapacityByteArrayOutputStream(int initialSlabSize) {
    this(initialSlabSize, 1024 * 1024);
  }

  /**
   * @param initialSlabSize the size to make the first slab
   * @param maxCapacityHint a hint (not guarantee) of the max amount of data written to this stream
   */
  public CapacityByteArrayOutputStream(int initialSlabSize, int maxCapacityHint) {
    checkArgument(initialSlabSize > 0, "initialSlabSize must be > 0");
    checkArgument(maxCapacityHint > 0, "maxCapacityHint must be > 0");
    checkArgument(maxCapacityHint >= initialSlabSize, String.format("maxCapacityHint can't be less than initialSlabSize %d %d", initialSlabSize, maxCapacityHint));
    this.initialSlabSize = initialSlabSize;
    this.maxCapacityHint = maxCapacityHint;
    reset();
  }

  /**
   * the new slab is guaranteed to be at least minimumSize
   * @param minimumSize the size of the data we want to copy in the new slab
   */
  private void addSlab(int minimumSize) {
    int nextSlabSize;

    if (bytesUsed == 0) {
      nextSlabSize = initialSlabSize;
    } else if (bytesUsed > maxCapacityHint / 5) {
      // to avoid an overhead of up to twice the needed size, we get linear when approaching target page size
      nextSlabSize = maxCapacityHint / 5;
    } else {
      // double the size every time
      nextSlabSize = bytesUsed;
    }

    if (nextSlabSize < minimumSize) {
      if (Log.DEBUG) LOG.debug(format("slab size %,d too small for value of size %,d. Bumping up slab size", nextSlabSize, minimumSize));
      nextSlabSize = minimumSize;
    }

    if (Log.DEBUG) LOG.debug(format("used %d slabs, adding new slab of size %d", slabs.size(), nextSlabSize));

    this.currentSlab = new byte[nextSlabSize];
    this.slabs.add(currentSlab);
    this.bytesAllocated += nextSlabSize;
    this.currentSlabIndex = 0;
  }

  @Override
  public void write(int b) {
    if (currentSlabIndex == currentSlab.length) {
      addSlab(1);
    }
    currentSlab[currentSlabIndex] = (byte) b;
    currentSlabIndex += 1;
    bytesUsed += 1;
  }

  @Override
  public void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException(
          String.format("Given byte array of size %d, with requested length(%d) and offset(%d)", b.length, len, off));
    }
    if (currentSlabIndex + len >= currentSlab.length) {
      final int length1 = currentSlab.length - currentSlabIndex;
      arraycopy(b, off, currentSlab, currentSlabIndex, length1);
      final int length2 = len - length1;
      addSlab(length2);
      arraycopy(b, off + length1, currentSlab, currentSlabIndex, length2);
      currentSlabIndex = length2;
    } else {
      arraycopy(b, off, currentSlab, currentSlabIndex, len);
      currentSlabIndex += len;
    }
    bytesUsed += len;
  }

  /**
   * Writes the complete contents of this buffer to the specified output stream argument. the output
   * stream's write method <code>out.write(slab, 0, slab.length)</code>) will be called once per slab.
   *
   * @param      out   the output stream to which to write the data.
   * @exception  IOException  if an I/O error occurs.
   */
  public void writeTo(OutputStream out) throws IOException {
    for (int i = 0; i < slabs.size() - 1; i++) {
      final byte[] slab = slabs.get(i);
      out.write(slab);
    }
    out.write(currentSlab, 0, currentSlabIndex);
  }

  /**
   * @return The total size in bytes of data written to this stream.
   */
  public long size() {
    return bytesUsed;
  }

  /**
   *
   * @return The total size in bytes currently allocated for this stream.
   */
  public int getCapacity() {
    return bytesAllocated;
  }

  /**
   * When re-using an instance with reset, it will adjust slab size based on previous data size.
   * The intent is to reuse the same instance for the same type of data (for example, the same column).
   * The assumption is that the size in the buffer will be consistent.
   */
  public void reset() {
    // readjust slab size.
    // 7 = 2^3 - 1 so that doubling the initial size 3 times will get to the same size
    this.initialSlabSize = max(bytesUsed / 7, initialSlabSize);
    if (Log.DEBUG) LOG.debug(String.format("initial slab of size %d", initialSlabSize));
    this.slabs.clear();
    this.bytesAllocated = 0;
    this.bytesUsed = 0;
    this.currentSlab = EMPTY_SLAB;
    this.currentSlabIndex = 0;
  }

  /**
   * @return the index of the last value written to this stream, which
   * can be passed to {@link #setByte(long, byte)} in order to change it
   */
  public long getCurrentIndex() {
    checkArgument(bytesUsed > 0, "This is an empty stream");
    return bytesUsed - 1;
  }

  /**
   * Replace the byte stored at position index in this stream with value
   *
   * @param index which byte to replace
   * @param value the value to replace it with
   */
  public void setByte(long index, byte value) {
    checkArgument(index < bytesUsed, "Index: " + index + " is >= the current size of: " + bytesUsed);

    long seen = 0;
    for (int i = 0; i < slabs.size(); i++) {
      byte[] slab = slabs.get(i);
      if (index < seen + slab.length) {
        // ok found index
        slab[(int)(index-seen)] = value;
        break;
      }
      seen += slab.length;
    }
  }

  /**
   * @param prefix  a prefix to be used for every new line in the string
   * @return a text representation of the memory usage of this structure
   */
  public String memUsageString(String prefix) {
    return format("%s %s %d slabs, %,d bytes", prefix, getClass().getSimpleName(), slabs.size(), getCapacity());
  }

  /**
   * @return the total number of allocated slabs
   */
  int getSlabCount() {
    return slabs.size();
  }
}
