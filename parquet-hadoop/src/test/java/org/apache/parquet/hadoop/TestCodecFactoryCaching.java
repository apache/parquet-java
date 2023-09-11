/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.checkerframework.checker.units.qual.C;
import org.junit.Assert;
import org.junit.Test;

public class TestCodecFactoryCaching {
  CodecFactory factory;

  private class CodecWorker extends Thread {
    CodecFactory.BytesCompressor myCompressor;
    CodecFactory.BytesDecompressor myDecompressor;

    @Override
    public void run() {
      myCompressor = factory.getCompressor(CompressionCodecName.GZIP);
      myDecompressor = factory.getDecompressor(CompressionCodecName.SNAPPY);
    }
  }

  @Test
  public void testThreadSafeCodecFactory() throws InterruptedException {
    factory = new CodecFactory(new Configuration(), 65536);
    Thread[] workers = new Thread[10];

    CodecFactory.BytesCompressor myCompressor = factory.getCompressor(CompressionCodecName.GZIP);
    CodecFactory.BytesDecompressor myDecompressor = factory.getDecompressor(CompressionCodecName.SNAPPY);

    // Create and launch workers
    for (int i=0; i<10; i++) {
      workers[i] = new CodecWorker();
      workers[i].start();
    }

    // Wait for workers to finish
    for (int i=0; i<10; i++) {
      workers[i].join();
    }

    // There should be 22 codecs cached in the factory
    Assert.assertEquals("Number of codecs before release", 22, factory.countCachedCodecs());

    // Release all codecs created by all threads
    factory.release();

    // There should be 0 codecs now
    Assert.assertEquals("Number of codecs after release", 0, factory.countCachedCodecs());
  }
}
