package parquet.hadoop.codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.xerial.snappy.Snappy;

import parquet.Preconditions;

/**
 * This class is a wrapper around the snappy compressor. It always consumes the
 * entire input in setInput and compresses it as one compressed block.
 */
public class SnappyCompressor implements Compressor {
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
        outputBuffer = ByteBuffer.allocateDirect(maxOutputSize);
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

    if (inputBuffer.remaining() < len) {
      ByteBuffer tmp = ByteBuffer.allocateDirect(inputBuffer.capacity() + len);
      inputBuffer.rewind();
      tmp.put(inputBuffer);
      inputBuffer = tmp;
    }

    // Append the current bytes to the input buffer
    inputBuffer.put(buffer, off, len);
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
    inputBuffer.clear();
    inputBuffer.limit(0);
    outputBuffer.clear();
    outputBuffer.limit(0);		
  }

  @Override
  public void setDictionary(byte[] dictionary, int off, int len) {
    // No-op		
  }
}
