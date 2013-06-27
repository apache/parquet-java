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

    if (needsInput()) {    	
      // No buffered output bytes and no input to consume, need more input
      return 0;
    }
    
    if (!outputBuffer.hasRemaining()) {
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
      // We've compressed the entire input, reset the input now
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

    if (inputBuffer.capacity() < len) {
      inputBuffer = ByteBuffer.allocateDirect(len);
    }

    // Clear the input direct buffer and put all the input data there.
    inputBuffer.clear();
    inputBuffer.put(buffer, off, len);
    inputBuffer.rewind();
    inputBuffer.limit(len);
  }

  @Override
  public void end() {
    // No-op		
  }

  @Override
  public boolean finished() {
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
    inputBuffer.clear();
    inputBuffer.limit(0);
    outputBuffer.clear();
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
