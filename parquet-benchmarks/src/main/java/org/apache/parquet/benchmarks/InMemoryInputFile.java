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
package org.apache.parquet.benchmarks;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

/**
 * An {@link InputFile} backed by an in-memory byte array. Useful for read benchmarks
 * that need to isolate decoding and decompression CPU cost from filesystem I/O.
 * Pair with {@link InMemoryOutputFile} to produce the byte array during setup.
 */
public final class InMemoryInputFile implements InputFile {

  private final byte[] data;

  public InMemoryInputFile(byte[] data) {
    this.data = data;
  }

  @Override
  public long getLength() {
    return data.length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new SeekableInputStream() {
      private int pos = 0;

      @Override
      public int read() {
        return pos < data.length ? (data[pos++] & 0xFF) : -1;
      }

      @Override
      public int read(byte[] b, int off, int len) {
        int remaining = data.length - pos;
        if (remaining <= 0) return -1;
        int n = Math.min(len, remaining);
        System.arraycopy(data, pos, b, off, n);
        pos += n;
        return n;
      }

      @Override
      public long getPos() {
        return pos;
      }

      @Override
      public void seek(long newPos) {
        pos = (int) newPos;
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        if (pos + len > data.length) {
          throw new EOFException("Unexpected end of data");
        }
        System.arraycopy(data, pos, bytes, start, len);
        pos += len;
      }

      @Override
      public int read(ByteBuffer buf) {
        int len = buf.remaining();
        int remaining = data.length - pos;
        if (remaining <= 0) return -1;
        int n = Math.min(len, remaining);
        buf.put(data, pos, n);
        pos += n;
        return n;
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        int len = buf.remaining();
        if (pos + len > data.length) {
          throw new EOFException("Unexpected end of data");
        }
        buf.put(data, pos, len);
        pos += len;
      }

      @Override
      public void close() {}
    };
  }
}
