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
package org.apache.parquet.hadoop.codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.xerial.snappy.Snappy;

import org.apache.parquet.Preconditions;

/**
 * This class is a wrapper around the snappy compressor. It always consumes the
 * entire input in setInput and compresses it as one compressed block.
 */
public class SnappyCompressor implements Compressor {
  // Double up to an 8 mb write buffer,  then switch to 1MB linear allocation
  private static final int DOUBLING_ALLOC_THRESH =  8 << 20;
  private static final int LINEAR_ALLOC_STEP = 1 << 20;

  // Buffer for compressed output. This buffer grows as necessary.
  private ByteBuffer outputBuffer = ByteBuffer.allocateDirect(0);

  // Buffer for uncompressed input. This buffer grows as necessary.
  private ByteBuffer inputBuffer = ByteBuffer.allocateDirect(0);

  private long bytesRead = 0L;
  private long bytesWritten = 0L;
  private boolean finishCalled = false;

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param buffer   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public synchronized int compress(byte[] buffer, int off, int len) throws IOException {
    SnappyUtil.validateBuffer(buffer, off, len);

    if (needsInput()) {
      // No buffered output bytes and no input to consume, need more input
      return 0;
    }

    if (!outputBuffer.hasRemaining()) {
      // There is uncompressed input, compress it now
      int maxOutputSize = Snappy.maxCompressedLength(inputBuffer.position());
      if (maxOutputSize > outputBuffer.capacity()) {
        ByteBuffer oldBuffer = outputBuffer;
        outputBuffer = ByteBuffer.allocateDirect(maxOutputSize);
        CleanUtil.cleanDirectBuffer(oldBuffer);
      }
      // Reset the previous outputBuffer
      outputBuffer.clear();
      inputBuffer.limit(inputBuffer.position());
      inputBuffer.position(0);

      int size = Snappy.compress(inputBuffer, outputBuffer);
      outputBuffer.limit(size);
      inputBuffer.limit(0);
      inputBuffer.rewind();
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.remaining());
    outputBuffer.get(buffer, off, numBytes);    
    bytesWritten += numBytes;
    return numBytes;	    
  }

  @Override
  public synchronized void setInput(byte[] buffer, int off, int len) {  
    SnappyUtil.validateBuffer(buffer, off, len);
    
    Preconditions.checkArgument(!outputBuffer.hasRemaining(), 
        "Output buffer should be empty. Caller must call compress()");

    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      resizeInputBuffer(inputBuffer.position() + len);
    }

    inputBuffer.limit(inputBuffer.position() + len);

    // Append the current bytes to the input buffer
    inputBuffer.put(buffer, off, len);
    bytesRead += len;
  }

  /**
   * Resize the internal input buffer to ensure enough write space.
   * @param requiredSizeBytes the minimum required capacity of the input buffer.
   */
  private void resizeInputBuffer(final int requiredSizeBytes) {
    int newCapacity = inputBuffer.capacity() == 0 ? requiredSizeBytes : inputBuffer.capacity();
    while(newCapacity < requiredSizeBytes && newCapacity < DOUBLING_ALLOC_THRESH) {
      newCapacity *= 2;
    }

    if (newCapacity < requiredSizeBytes) {
      final int delta = requiredSizeBytes - DOUBLING_ALLOC_THRESH;
      final int steps = (delta + LINEAR_ALLOC_STEP - 1) / LINEAR_ALLOC_STEP;
      newCapacity = DOUBLING_ALLOC_THRESH + steps * LINEAR_ALLOC_STEP;
    }

    final ByteBuffer tmp = ByteBuffer.allocateDirect(newCapacity);
    inputBuffer.rewind();
    tmp.put(inputBuffer);
    final ByteBuffer oldBuffer = inputBuffer;
    inputBuffer = tmp;
    CleanUtil.cleanDirectBuffer(oldBuffer);
  }

  @Override
  public void end() {
    CleanUtil.cleanDirectBuffer(inputBuffer);
    CleanUtil.cleanDirectBuffer(outputBuffer);
  }

  @Override
  public synchronized void finish() {
    finishCalled = true;
  }

  @Override
  public synchronized boolean finished() {
    return finishCalled && inputBuffer.position() == 0 && !outputBuffer.hasRemaining();
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  // We want to compress all the input in one go so we always need input until it is
  // all consumed.
  public synchronized boolean needsInput() {
    return !finishCalled;
  }

  @Override
  public void reinit(Configuration c) {
    reset();		
  }

  @Override
  public synchronized void reset() {
    finishCalled = false;
    bytesRead = bytesWritten = 0;
    inputBuffer.rewind();
    outputBuffer.rewind();
    inputBuffer.limit(0);
    outputBuffer.limit(0);
  }

  @Override
  public void setDictionary(byte[] dictionary, int off, int len) {
    // No-op		
  }
}
