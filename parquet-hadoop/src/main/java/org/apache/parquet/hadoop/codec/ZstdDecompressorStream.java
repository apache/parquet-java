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

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStream;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

public class ZstdDecompressorStream extends CompressionInputStream {

  private ZstdInputStream zstdInputStream;

  public ZstdDecompressorStream(InputStream stream) throws IOException {
	  super(stream);
    zstdInputStream = new ZstdInputStream(stream);
  }

  public ZstdDecompressorStream(InputStream stream, BufferPool pool) throws IOException {
    super(stream);
    zstdInputStream = new ZstdInputStream(stream, pool);
  }

  public int read(byte[] b, int off, int len) throws IOException {
    return zstdInputStream.read(b, off, len);
  }

  public int read() throws IOException {
    return zstdInputStream.read();
  }

  public void resetState() throws IOException {
    // no-opt, doesn't apply to ZSTD
  }
}
