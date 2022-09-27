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
package org.apache.parquet.hadoop;

import org.apache.parquet.hadoop.codec.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestLz4RawCodec {
  @Test
  public void TestLz4Raw() throws IOException {
    // Reuse the snappy objects between test cases
    Lz4RawCompressor compressor = new Lz4RawCompressor();
    Lz4RawDecompressor decompressor = new Lz4RawDecompressor();

    TestLz4Raw(compressor, decompressor, "");
    TestLz4Raw(compressor, decompressor, "FooBar");
    TestLz4Raw(compressor, decompressor, "FooBar1", "FooBar2");
    TestLz4Raw(compressor, decompressor, "FooBar");
    TestLz4Raw(compressor, decompressor, "a", "blahblahblah", "abcdef");
    TestLz4Raw(compressor, decompressor, "");
    TestLz4Raw(compressor, decompressor, "FooBar");
  }

  private void TestLz4Raw(Lz4RawCompressor compressor, Lz4RawDecompressor decompressor,
                          String... strings) throws IOException {
    compressor.reset();
    decompressor.reset();

    int uncompressedSize = 0;
    for (String s : strings) {
      uncompressedSize += s.length();
    }
    byte[] uncompressedData = new byte[uncompressedSize];
    int len = 0;
    for (String s : strings) {
      byte[] tmp = s.getBytes();
      System.arraycopy(tmp, 0, uncompressedData, len, s.length());
      len += s.length();
    }

    assert (compressor.needsInput());
    compressor.setInput(uncompressedData, 0, len);
    assert (compressor.needsInput());
    compressor.finish();
    assert (!compressor.needsInput());
    assert (!compressor.finished() || uncompressedSize == 0);
    byte[] compressedData = new byte[1000];

    int compressedSize = compressor.compress(compressedData, 0, 1000);
    assert (compressor.finished());

    assert (!decompressor.finished());
    assert (decompressor.needsInput());
    decompressor.setInput(compressedData, 0, compressedSize);
    assert (!decompressor.finished());
    byte[] decompressedData = new byte[uncompressedSize];
    int decompressedSize = decompressor.decompress(decompressedData, 0, uncompressedSize);
    assert (decompressor.finished());

    assertEquals(uncompressedSize, decompressedSize);
    assertArrayEquals(uncompressedData, decompressedData);
  }
}
