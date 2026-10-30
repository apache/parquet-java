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

  private final BrotliOutputStream encoder;
  private boolean finished;
  private boolean closed;
  private IOException failure;

  BrotliCompressionOutputStream(OutputStream out, int quality) throws IOException {
    super(out);
    this.encoder = new BrotliOutputStream(new NonClosingOutputStream(out), Encoder.Parameters.create(quality));
  }

  @Override
  public void write(int b) throws IOException {
    ensureWritable();
    try {
      encoder.write(b);
    } catch (IOException e) {
      failure = e;
      throw e;
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureWritable();
    try {
      encoder.write(b, off, len);
    } catch (IOException e) {
      failure = e;
      throw e;
    }
  }

  @Override
  public void flush() throws IOException {
    ensureNoFailure();
    try {
      if (!finished) {
        encoder.flush();
      }
      out.flush();
    } catch (IOException e) {
      failure = e;
      throw e;
    }
  }

  @Override
  public void finish() throws IOException {
    ensureNoFailure();
    finishEncoder();
  }

  private void finishEncoder() throws IOException {
    if (finished) {
      return;
    }
    try {
      encoder.close();
      finished = true;
    } catch (IOException e) {
      if (failure == null) {
        failure = e;
      }
      throw e;
    }
  }

  @Override
  public void resetState() throws IOException {
    throw new IOException("BrotliCompressionOutputStream does not support resetState");
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    // Use a distinct exception so try-with-resources can suppress it onto the original failure.
    IOException closeFailure =
        failure == null ? null : new IOException("Previous Brotli stream operation failed", failure);
    try {
      // Release native resources even when a previous write or flush has failed.
      finishEncoder();
    } catch (IOException e) {
      if (closeFailure == null) {
        closeFailure = e;
      } else if (closeFailure != e && closeFailure.getCause() != e) {
        closeFailure.addSuppressed(e);
      }
    }
    try {
      out.close();
    } catch (IOException e) {
      if (closeFailure == null) {
        closeFailure = e;
      } else if (closeFailure != e && closeFailure.getCause() != e) {
        closeFailure.addSuppressed(e);
      }
    }
    closed = true;
    if (closeFailure != null) {
      throw closeFailure;
    }
  }

  private void ensureWritable() throws IOException {
    ensureNoFailure();
    if (finished) {
      throw new IOException("Stream is finished");
    }
  }

  private void ensureNoFailure() throws IOException {
    if (failure != null) {
      throw new IOException("Previous Brotli stream operation failed", failure);
    }
  }

  /**
   * BrotliOutputStream closes its target when it finalizes the Brotli stream. Hadoop finishes a
   * codec stream without closing the caller's target, so swallow close here and only flush. The
   * codec's own close() closes the target explicitly.
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
