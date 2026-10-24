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

import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.encoder.Encoder;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

class BrotliCompressionOutputStream extends CompressionOutputStream {

  private final BrotliOutputStream out;
  private boolean finished;

  BrotliCompressionOutputStream(OutputStream out, int quality) throws IOException {
    super(out);
    this.out = new BrotliOutputStream(new NonClosingOutputStream(out), Encoder.Parameters.create(quality));
  }

  @Override
  public void write(int b) throws IOException {
    ensureOpen();
    out.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureOpen();
    out.write(b, off, len);
  }

  @Override
  public void finish() throws IOException {
    if (!finished) {
      out.close();
      finished = true;
    }
  }

  @Override
  public void resetState() throws IOException {
    throw new IOException("BrotliCompressionOutputStream does not support resetState");
  }

  @Override
  public void close() throws IOException {
    finish();
  }

  private void ensureOpen() throws IOException {
    if (finished) {
      throw new IOException("Stream is finished");
    }
  }

  /**
   * BrotliOutputStream closes its target when it finalizes the Brotli stream. Hadoop finishes a
   * codec stream without closing the caller's target, so swallow close here and only flush.
   */
  private static final class NonClosingOutputStream extends FilterOutputStream {

    NonClosingOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      flush();
    }
  }
}
