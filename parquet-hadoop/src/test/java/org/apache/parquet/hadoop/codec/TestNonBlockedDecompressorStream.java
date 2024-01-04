package org.apache.parquet.hadoop.codec;

import org.apache.hadoop.io.compress.Decompressor;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.Mockito.when;

public class TestNonBlockedDecompressorStream {

  @Test
  public void testZeroBytesRead() {
    try {
      // Create a mock Decompressor that returns 0 when decompress is called.
      Decompressor mockDecompressor = Mockito.mock(Decompressor.class);
      when(mockDecompressor.decompress(Mockito.any(byte[].class), Mockito.anyInt(),
          Mockito.anyInt())).thenReturn(0);

      // Create a NonBlockedDecompressorStream with the mock Decompressor.
      NonBlockedDecompressorStream decompressorStream = new NonBlockedDecompressorStream(
          new ByteArrayInputStream(new byte[0]), mockDecompressor, 1024);

      // Attempt to read from the stream, which should trigger the IOException.
      decompressorStream.read(new byte[1024], 0, 1024);

      // If no exception is thrown, then the test fails.
      Assert.fail("Expected an IOException to be thrown");
    } catch (IOException e) {
      Assert.assertEquals("Zero bytes read during decompression, suggesting a potential file corruption.",
          e.getMessage());
    }
  }
}
