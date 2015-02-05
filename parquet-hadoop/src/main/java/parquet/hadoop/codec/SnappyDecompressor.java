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
package parquet.hadoop.codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.xerial.snappy.Snappy;

import parquet.Preconditions;

public class SnappyDecompressor implements Decompressor {
  // Buffer for uncompressed output. This buffer grows as necessary.
  private ByteBuffer outputBuffer = ByteBuffer.allocateDirect(0);

  // Buffer for compressed input. This buffer grows as necessary.
  private ByteBuffer inputBuffer = ByteBuffer.allocateDirect(0);

  private boolean finished;
  
  /**
   * Fills specified buffer with uncompressed data. Returns actual number
   * of bytes of uncompressed data. A return value of 0 indicates that
   * {@link #needsInput()} should be called in order to determine if more
   * input data is required.
   *
   * @param buffer   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of uncompressed data.
   * @throws IOException
   */
  @Override
  public synchronized int decompress(byte[] buffer, int off, int len) throws IOException {
    SnappyUtil.validateBuffer(buffer, off, len);
	if (inputBuffer.position() == 0 && !outputBuffer.hasRemaining()) {
      return 0;
    }
    
    if (!outputBuffer.hasRemaining()) {
      inputBuffer.rewind();
      Preconditions.checkArgument(inputBuffer.position() == 0, "Invalid position of 0.");
      Preconditions.checkArgument(outputBuffer.position() == 0, "Invalid position of 0.");
      // There is compressed input, decompress it now.
      int decompressedSize = Snappy.uncompressedLength(inputBuffer);
      if (decompressedSize > outputBuffer.capacity()) {
        outputBuffer = ByteBuffer.allocateDirect(decompressedSize);
      }

      // Reset the previous outputBuffer (i.e. set position to 0)
      outputBuffer.clear();
      int size = Snappy.uncompress(inputBuffer, outputBuffer);
      outputBuffer.limit(size);
      // We've decompressed the entire input, reset the input now
      inputBuffer.clear();
      inputBuffer.limit(0);
      finished = true;
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.remaining());
    outputBuffer.get(buffer, off, numBytes);
    return numBytes;	    
  }

  /**
   * Sets input data for decompression.
   * This should be called if and only if {@link #needsInput()} returns
   * <code>true</code> indicating that more input data is required.
   * (Both native and non-native versions of various Decompressors require
   * that the data passed in via <code>b[]</code> remain unmodified until
   * the caller is explicitly notified--via {@link #needsInput()}--that the
   * buffer may be safely modified.  With this requirement, an extra
   * buffer-copy can be avoided.)
   *
   * @param buffer   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public synchronized void setInput(byte[] buffer, int off, int len) {
    SnappyUtil.validateBuffer(buffer, off, len);

    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      ByteBuffer newBuffer = ByteBuffer.allocateDirect(inputBuffer.position() + len);
      inputBuffer.rewind();
      newBuffer.put(inputBuffer);
      inputBuffer = newBuffer;      
    } else {
      inputBuffer.limit(inputBuffer.position() + len);
    }
    inputBuffer.put(buffer, off, len);
  }

  @Override
  public void end() {
    // No-op		
  }

  @Override
  public synchronized boolean finished() {
    return finished && !outputBuffer.hasRemaining();
  }

  @Override
  public int getRemaining() {
    return 0;
  }

  @Override
  public synchronized boolean needsInput() {
    return !inputBuffer.hasRemaining() && !outputBuffer.hasRemaining();
  }

  @Override
  public synchronized void reset() {
    finished = false;
    inputBuffer.rewind();
    outputBuffer.rewind();
    inputBuffer.limit(0);
    outputBuffer.limit(0);
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // No-op		
  }
}
