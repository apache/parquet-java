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

import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.hadoop.io.compress.Decompressor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNonBlockedDecompressorStream {

  @Test
  public void testZeroBytesRead() {
    try {
      // Create a mock Decompressor that returns 0 when decompress is called.
      Decompressor mockDecompressor = Mockito.mock(Decompressor.class);
      when(mockDecompressor.decompress(Mockito.any(byte[].class), Mockito.anyInt(), Mockito.anyInt()))
          .thenReturn(0);

      // Create a NonBlockedDecompressorStream with the mock Decompressor.
      NonBlockedDecompressorStream decompressorStream =
          new NonBlockedDecompressorStream(new ByteArrayInputStream(new byte[0]), mockDecompressor, 1024);

      // Attempt to read from the stream, which should trigger the IOException.
      decompressorStream.read(new byte[1024], 0, 1024);

      // If no exception is thrown, then the test fails.
      Assert.fail("Expected an IOException to be thrown");
    } catch (IOException e) {
      Assert.assertEquals(
          "Zero bytes read during decompression, suggesting a potential file corruption.", e.getMessage());
    }
  }
}
