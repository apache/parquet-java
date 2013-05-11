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

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import parquet.Log;

/**
 * functionality of ByteArrayOutputStream without the memory and copy overhead
 *
 * @author Julien Le Dem
 *
 */
public class CapacityByteArrayOutputStream extends OutputStream {
  private static final Log LOG = Log.getLog(CapacityByteArrayOutputStream.class);

  private int slabSize;
  private List<byte[]> slabs = new ArrayList<byte[]>();
  private byte[] currentSlab;
  private int capacity;
  private int currentSlabIndex;
  private int currentSlabPosition;
  private int size;

  public CapacityByteArrayOutputStream(int initialSize) {
    initSlabs(initialSize);
  }

  private void initSlabs(int initialSize) {
    if (Log.DEBUG) LOG.debug(String.format("initial slab of size %d", initialSize));
    this.slabSize = initialSize;
    this.slabs.clear();
    this.capacity = initialSize;
    this.currentSlab = new byte[slabSize];
    this.slabs.add(currentSlab);
    this.currentSlabIndex = 0;
    this.currentSlabPosition = 0;
    this.size = 0;
  }

  private void addSlab(int minimumSize) {
    this.currentSlabIndex += 1;
    if (currentSlabIndex < this.slabs.size()) {
      // reuse existing slab
      this.currentSlab = this.slabs.get(currentSlabIndex);
      if (Log.DEBUG) LOG.debug(String.format("reusing slab of size %d", currentSlab.length));
      if (currentSlab.length < minimumSize) {
        if (Log.DEBUG) LOG.debug(String.format("slab size %,d too small for value of size %,d. Bumping up slab size", currentSlab.length, minimumSize));
        byte[] newSlab = new byte[minimumSize];
        capacity += minimumSize - currentSlab.length;
        this.currentSlab = newSlab;
        this.slabs.set(currentSlabIndex, newSlab);
      }
    } else {
      if (currentSlabIndex > 10) {
        // make slabs bigger in case we are creating too many of them
        // double slab size every time.
        this.slabSize = size;
        if (Log.DEBUG) LOG.debug(String.format("used %d slabs, new slab size %d", currentSlabIndex, slabSize));
      }
      if (slabSize < minimumSize) {
        if (Log.DEBUG) LOG.debug(String.format("slab size %,d too small for value of size %,d. Bumping up slab size", slabSize, minimumSize));
        this.slabSize = minimumSize;
      }
      if (Log.DEBUG) LOG.debug(String.format("new slab of size %d", slabSize));
      this.currentSlab = new byte[slabSize];
      this.slabs.add(currentSlab);
      this.capacity += slabSize;
    }
    this.currentSlabPosition = 0;
  }

  /**
   * Writes the specified byte to this byte array output stream.
   *
   * @param   b   the byte to be written.
   */
  public void write(int b) {
    if (currentSlabPosition == currentSlab.length) {
      addSlab(1);
    }
    currentSlab[currentSlabPosition] = (byte) b;
    currentSlabPosition += 1;
    size += 1;
  }

  /**
   * Writes <code>len</code> bytes from the specified byte array
   * starting at offset <code>off</code> to this byte array output stream.
   *
   * @param   b     the data.
   * @param   off   the start offset in the data.
   * @param   len   the number of bytes to write.
   */
  public void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) ||
        ((off + len) - b.length > 0)) {
      throw new IndexOutOfBoundsException();
    }
    if (currentSlabPosition + len >= currentSlab.length) {
      final int length1 = currentSlab.length - currentSlabPosition;
      System.arraycopy(b, off, currentSlab, currentSlabPosition, length1);
      final int length2 = len - length1;
      addSlab(length2);
      System.arraycopy(b, off + length1, currentSlab, currentSlabPosition, length2);
      currentSlabPosition = length2;
    } else {
      System.arraycopy(b, off, currentSlab, currentSlabPosition, len);
      currentSlabPosition += len;
    }
    size += len;
  }

  /**
   * Writes the complete contents of this byte array output stream to
   * the specified output stream argument, as if by calling the output
   * stream's write method using <code>out.write(buf, 0, count)</code>.
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
   *
   * @return the size of the allocated buffer
   */
  public int getCapacity() {
    return capacity;
  }

  public void reset() {
    // heuristics to adjust slab size
    if (
        // if we have only one slab, make sure it is not way too big
        (currentSlabIndex == 0 && currentSlabPosition < currentSlab.length / 2 && currentSlab.length > 64 * 1024)
        ||
        // we want to avoid generating too many slabs.
        (currentSlabIndex > 10)
        ){
      // readjust slab size
      initSlabs(Math.max(size / 5, 64 * 1024)); // should make overhead to about 20% without incurring many slabs
      if (Log.DEBUG) LOG.debug(String.format("used %d slabs, new slab size %d", currentSlabIndex + 1, slabSize));
    } else if (currentSlabIndex < slabs.size() - 1) {
      // free up the slabs that we are not using. We want to minimize overhead
      this.slabs = new ArrayList<byte[]>(slabs.subList(0, currentSlabIndex + 1));
      this.capacity = 0;
      for (byte[] slab : slabs) {
        capacity += slab.length;
      }
    }
    this.currentSlabIndex = 0;
    this.currentSlabPosition = 0;
    this.currentSlab = slabs.get(currentSlabIndex);
    this.size = 0;
  }

  public long size() {
    return size;
  }

  public String memUsageString(String prefix) {
    return String.format("%s %s %d slabs, %,d bytes", prefix, getClass().getSimpleName(), slabs.size(), getCapacity());
  }
}