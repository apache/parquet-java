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
package parquet.hadoop.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.xerial.snappy.Snappy;
import parquet.Preconditions;
import parquet.hadoop.codec.buffers.CodecByteBuffer;
import parquet.hadoop.codec.buffers.CodecByteBufferFactory;
import parquet.hadoop.codec.buffers.CodecByteBufferFactory.BuffReuseOpt;

import java.io.IOException;
/**
 * This class is a wrapper around the snappy compressor. It always consumes the
 * entire input in setInput and compresses it as one compressed block.
 */
public class SnappyCompressor implements Compressor {
  // Buffer for compressed output. This buffer grows as necessary.
  private CodecByteBuffer outputBuffer;

  // Buffer for uncompressed input. This buffer grows as necessary.
  private CodecByteBuffer inputBuffer;


  private CodecByteBufferFactory byteBufferFactory;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;
  private boolean finishCalled = false;

  public SnappyCompressor() {
    // default buffer behaviour unless overridden
    this.byteBufferFactory = new CodecByteBufferFactory(BuffReuseOpt.ReuseOnReset);
    inputBuffer = byteBufferFactory.create(0);
    outputBuffer = byteBufferFactory.create(0);
  }

  public void setByteBufferFactory(CodecByteBufferFactory byteBufferFactory) {
    this.byteBufferFactory = byteBufferFactory;
  }

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
      int maxOutputSize = Snappy.maxCompressedLength(inputBuffer.get().position());
      if (maxOutputSize > outputBuffer.get().capacity()) {
        outputBuffer.freeBuffer();
        outputBuffer = byteBufferFactory.create(maxOutputSize);
      }
      // Reset the previous outputBuffer.get()
      outputBuffer.get().clear();
      inputBuffer.get().limit(inputBuffer.get().position());
      inputBuffer.get().position(0);

      int size = Snappy.compress(inputBuffer.get(), outputBuffer.get());
      outputBuffer.get().limit(size);
      inputBuffer.resetBuffer();
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.get().remaining());
    outputBuffer.get().get(buffer, off, numBytes);
    bytesWritten += numBytes;

    if (!outputBuffer.hasRemaining()) {
      outputBuffer.resetBuffer();
    }
    return numBytes;	    
  }

  @Override
  public synchronized void setInput(byte[] buffer, int off, int len) {  
    SnappyUtil.validateBuffer(buffer, off, len);
    
    Preconditions.checkArgument(!outputBuffer.get().hasRemaining(),
        "Output buffer should be empty. Caller must call compress()");

    if (inputBuffer.get().capacity() - inputBuffer.get().position() < len) {
      CodecByteBuffer tmp = byteBufferFactory.create(inputBuffer.get().position() + len);
      inputBuffer.get().rewind();
      tmp.get().put(inputBuffer.get());
      inputBuffer.freeBuffer();
      inputBuffer = tmp;
    } else {
      inputBuffer.get().limit(inputBuffer.get().position() + len);
    }

    // Append the current bytes to the input buffer
    inputBuffer.get().put(buffer, off, len);
    bytesRead += len;
  }

  @Override
  public void end() {
    // No-op		
  }

  @Override
  public void finish() {
    finishCalled = true;
  }

  @Override
  public synchronized boolean finished() {
    return finishCalled && !outputBuffer.hasRemaining() && inputBuffer.get().position() == 0;
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
    inputBuffer.get().rewind();
    outputBuffer.get().rewind();
    inputBuffer.get().limit(0);
    outputBuffer.get().limit(0);
  }

  @Override
  public void setDictionary(byte[] dictionary, int off, int len) {
    // No-op		
  }



}
