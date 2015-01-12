package parquet.hadoop.codec.buffers;

import java.nio.ByteBuffer;

/**
 * Interface abstracts out the difference between reusing the same
 * byte buffer every time, and freeing/reallocating the buffer as required
 * to save on memory overheads (at the cost some cpu overhead for memory allocating)
 */
public interface CodecByteBuffer {
  /**
   * Get the underlying ByteBuffer
   * @return byteBuffer
   */
  ByteBuffer get();

  /**
   * indicate the buffer is not being used anymore
   */
  void resetBuffer();

  /**
   * Is there any data in the buffer?
   * @return True if data remains, false otherwise
   */
  boolean hasRemaining();

  /**
   * Explicitly free the buffer
   */
  void freeBuffer();

}
