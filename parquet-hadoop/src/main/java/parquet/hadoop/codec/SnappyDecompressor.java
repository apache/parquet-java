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

import org.apache.hadoop.io.compress.Decompressor;
import org.xerial.snappy.Snappy;
import parquet.Preconditions;
import parquet.hadoop.codec.buffers.CodecByteBuffer;
import parquet.hadoop.codec.buffers.CodecByteBufferFactory;
import parquet.hadoop.codec.buffers.CodecByteBufferFactory.BuffReuseOpt;

import java.io.IOException;

public class SnappyDecompressor implements Decompressor {
  // Buffer for uncompressed output. This buffer grows as necessary.
  private CodecByteBuffer outputBuffer;

  // Buffer for compressed input. This buffer grows as necessary.
  private CodecByteBuffer inputBuffer;

  private boolean finished;

  private CodecByteBufferFactory byteBufferFactory;

  public SnappyDecompressor() {
    // default buffer behaviour unless overridden
    this.byteBufferFactory = new CodecByteBufferFactory(BuffReuseOpt.ReuseOnReset);
    outputBuffer = byteBufferFactory.create(0);
    inputBuffer = byteBufferFactory.create(0);
  }

  public void setByteBufferFactory(final CodecByteBufferFactory byteBufferFactory) {
    this.byteBufferFactory = byteBufferFactory;
  }

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
	  if (!outputBuffer.hasRemaining() && inputBuffer.get().position() == 0) {
      return 0;
    }
    
    if (!outputBuffer.hasRemaining()) {
      inputBuffer.get().rewind();
      Preconditions.checkArgument(inputBuffer.get().position() == 0, "Invalid position of 0.");
      Preconditions.checkArgument(outputBuffer.get().position() == 0, "Invalid position of 0.");
      // There is compressed input, decompress it now.
      int decompressedSize = Snappy.uncompressedLength(inputBuffer.get());
      if (decompressedSize > outputBuffer.get().capacity()) {
        outputBuffer = byteBufferFactory.create(decompressedSize);
      }

      // Reset the previous outputBuffer.get() (i.e. set position to 0)
      outputBuffer.get().clear();
      int size = Snappy.uncompress(inputBuffer.get(), outputBuffer.get());
      outputBuffer.get().limit(size);
      // We've decompressed the entire input, reset the input now
      inputBuffer.resetBuffer();
      finished = true;
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.get().remaining());
    outputBuffer.get().get(buffer, off, numBytes);

    if (!outputBuffer.hasRemaining()) {
      outputBuffer.resetBuffer();
    }
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

    if (inputBuffer.get().capacity() - inputBuffer.get().position() < len) {
      CodecByteBuffer newBuffer = byteBufferFactory.create(inputBuffer.get().position() + len);
      inputBuffer.get().rewind();
      newBuffer.get().put(inputBuffer.get());
      inputBuffer.freeBuffer();
      inputBuffer = newBuffer;
    } else {
      inputBuffer.get().limit(inputBuffer.get().position() + len);
    }
    inputBuffer.get().put(buffer, off, len);
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
    inputBuffer.get().rewind();
    outputBuffer.get().rewind();
    inputBuffer.get().limit(0);
    outputBuffer.get().limit(0);
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
