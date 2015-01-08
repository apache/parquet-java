/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.bytes;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static parquet.Preconditions.checkArgument;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import parquet.Log;

/**
 * functionality of ByteArrayOutputStream without the memory and copy overhead
 *
 * It will linearly create a new slab of the initial size when needed (instead of creating a new buffer and copying the data).
 * After 10 slabs their size will increase exponentially (similar to {@link ByteArrayOutputStream} behavior) by making the new slab size the size of the existing data.
 *
 * When reusing a buffer it will adjust the slab size based on the previous data size ({@link CapacityByteArrayOutputStream#reset()})
 *
 * @author Julien Le Dem
 *
 */
public class CapacityByteArrayOutputStream extends OutputStream {
  private static final Log LOG = Log.getLog(CapacityByteArrayOutputStream.class);

  private static final byte[] EMPTY_SLAB = new byte[0];

  private int initialSize;
  private final int pageSize;
  private List<byte[]> slabs = new ArrayList<byte[]>();
  private byte[] currentSlab;
  private int capacity = 0;
  private int currentSlabIndex;
  private int currentSlabPosition;
  private int size;

  /**
   * defaults pageSize to 1MB
   * @param initialSize
   * @deprecated use {@link CapacityByteArrayOutputStream#CapacityByteArrayOutputStream(int, int)}
   */
  @Deprecated
  public CapacityByteArrayOutputStream(int initialSize) {
    this(initialSize, 1024 * 1024);
  }

  /**
   * @param initialSize the initialSize of the buffer (also slab size)
   * @param pageSize
   */
  public CapacityByteArrayOutputStream(int initialSize, int pageSize) {
    checkArgument(initialSize > 0, "initialSize must be > 0");
    checkArgument(pageSize > 0, "pageSize must be > 0");
    this.pageSize = pageSize;
    initSlabs(initialSize);
  }

  private void initSlabs(int initialSize) {
    if (Log.DEBUG) LOG.debug(String.format("initial slab of size %d", initialSize));
    this.initialSize = initialSize;
    this.slabs.clear();
    this.capacity = 0;
    this.currentSlab = EMPTY_SLAB;
    this.currentSlabIndex = -1;
    this.currentSlabPosition = 0;
    this.size = 0;
  }

  /**
   * the new slab is guaranteed to be at least minimumSize
   * @param minimumSize the size of the data we want to copy in the new slab
   */
  private void addSlab(int minimumSize) {
    this.currentSlabIndex += 1;
    int nextSlabSize;
    if (size == 0) {
      nextSlabSize = initialSize;
    } else if (size > pageSize / 5) {
      // to avoid an overhead of up to twice the needed size, we get linear when approaching target page size
      nextSlabSize = pageSize / 5;
    } else {
      // double the size every time
      nextSlabSize = size;
    }
    if (nextSlabSize < minimumSize) {
      if (Log.DEBUG) LOG.debug(format("slab size %,d too small for value of size %,d. Bumping up slab size", nextSlabSize, minimumSize));
      nextSlabSize = minimumSize;
    }
    if (Log.DEBUG) LOG.debug(format("used %d slabs, new slab size %d", currentSlabIndex, nextSlabSize));
    this.currentSlab = new byte[nextSlabSize];
    this.slabs.add(currentSlab);
    this.capacity += nextSlabSize;
    this.currentSlabPosition = 0;
  }

  @Override
  public void write(int b) {
    if (currentSlabPosition == currentSlab.length) {
      addSlab(1);
    }
    currentSlab[currentSlabPosition] = (byte) b;
    currentSlabPosition += 1;
    size += 1;
  }

  @Override
  public void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (currentSlabPosition + len >= currentSlab.length) {
      final int length1 = currentSlab.length - currentSlabPosition;
      arraycopy(b, off, currentSlab, currentSlabPosition, length1);
      final int length2 = len - length1;
      addSlab(length2);
      arraycopy(b, off + length1, currentSlab, currentSlabPosition, length2);
      currentSlabPosition = length2;
    } else {
      arraycopy(b, off, currentSlab, currentSlabPosition, len);
      currentSlabPosition += len;
    }
    size += len;
  }

  /**
   * Writes the complete contents of this buffer to the specified output stream argument. the output
   * stream's write method <code>out.write(slab, 0, slab.length)</code>) will be called once per slab.
   *
   * @param      out   the output stream to which to write the data.
   * @exception  IOException  if an I/O error occurs.
   */
  public void writeTo(OutputStream out) throws IOException {
    for (int i = 0; i < currentSlabIndex; i++) {
      final byte[] slab = slabs.get(i);
      out.write(slab, 0, slab.length);
    }
    out.write(currentSlab, 0, currentSlabPosition);
  }

  /**
   * @return the size of the allocated buffer
   */
  public int getCapacity() {
    return capacity;
  }

  /**
   * When re-using an instance with reset, it will adjust slab size based on previous data size.
   * The intent is to reuse the same instance for the same type of data (for example, the same column).
   * The assumption is that the size in the buffer will be consistent.
   */
  public void reset() {
    // readjust slab size.
    // 7 = 2^3 - 1 so that doubling the initial size 3 times will get to the same size
    initSlabs(max(size / 7, initialSize));
  }

  /**
   * @return the size of the buffered data
   */
  public long size() {
    return size;
  }

  /**
   * @return the index of the last value being written to this stream, which
   * can be passed to {@link #setByte(long, byte)} in order to change it
   */
  public long getCurrentIndex() {
    checkArgument(size > 0, "This is an empty stream");
    return size - 1;
  }

  /**
   * Replace the byte stored at position index in this stream with value
   *
   * @param index which byte to replace
   * @param value the value to replace it with
   */
  public void setByte(long index, byte value) {
    checkArgument(index < size, "Index: " + index + " is >= the current size of: " + size);

    long seen = 0;
    for (int i = 0; i <= currentSlabIndex; i++) {
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
   * @return the total count of allocated slabs
   */
  int getSlabCount() {
    return slabs.size();
  }
}