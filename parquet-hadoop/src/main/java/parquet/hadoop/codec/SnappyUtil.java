package parquet.hadoop.codec;

import parquet.Preconditions;

/**
 * Utilities for SnappyCompressor and SnappyDecompressor.
 */
public class SnappyUtil {
  public static void validateBuffer(byte[] buffer, int off, int len) {
    Preconditions.checkNotNull(buffer, "buffer");
    Preconditions.checkArgument(off >= 0 && len >= 0 && off <= buffer.length - len,
        "Invalid offset or length. Out of buffer bounds.");
  }
}
