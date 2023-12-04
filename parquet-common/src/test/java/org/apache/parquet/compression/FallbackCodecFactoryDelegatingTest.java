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
package org.apache.parquet.compression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collection;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class FallbackCodecFactoryDelegatingTest {

  private final CompressionCodecFactory dummyCodecFactory = new CompressionCodecFactory() {

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
      return null;
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
      return null;
    }

    @Override
    public void release() {}
  };

  private final CompressionCodecFactory throwingCodecFactory = new CompressionCodecFactory() {

    boolean released = false;

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
      throw new CodecNotImplementedException(codecName.toString() + " compressor");
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
      throw new CodecNotImplementedException(codecName.toString() + " decompressor");
    }

    @Override
    public void release() {
      if (released) {
        throw new RuntimeException("Released for the second time!");
      }
      released = true;
    }
  };

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
      {CompressionCodecName.UNCOMPRESSED},
      {CompressionCodecName.SNAPPY},
      {CompressionCodecName.GZIP},
      {CompressionCodecName.LZO},
      {CompressionCodecName.BROTLI},
      {CompressionCodecName.LZ4},
      {CompressionCodecName.ZSTD},
      {CompressionCodecName.LZ4_RAW}
    };
    return Arrays.asList(data);
  }

  private final CompressionCodecName codecName;

  public FallbackCodecFactoryDelegatingTest(CompressionCodecName codecName) {
    this.codecName = codecName;
  }

  @Test
  public void skipsThrowingFactoryImplementation() {
    CompressionCodecFactory fallbackCodecFactory =
        new FallbackCodecFactory(throwingCodecFactory, dummyCodecFactory);
    assertEquals(fallbackCodecFactory.getCompressor(codecName), dummyCodecFactory.getCompressor(codecName));
    assertEquals(fallbackCodecFactory.getDecompressor(codecName), dummyCodecFactory.getDecompressor(codecName));
  }

  @Test
  public void releaseDelegatesToPassedFactories() {
    CompressionCodecFactory fallbackCodecFactory =
        new FallbackCodecFactory(dummyCodecFactory, throwingCodecFactory);
    fallbackCodecFactory.release();
    assertThrows(RuntimeException.class, throwingCodecFactory::release);
  }

  @Test
  public void releaseOnlyDelegatesToPassedFactories() {
    CompressionCodecFactory fallbackCodecFactory = new FallbackCodecFactory(dummyCodecFactory);
    fallbackCodecFactory.release();
    throwingCodecFactory.release();
  }
}
