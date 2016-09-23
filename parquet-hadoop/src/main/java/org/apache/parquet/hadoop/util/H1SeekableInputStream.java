/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.hadoop.util;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.io.SeekableInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * SeekableInputStream implementation that implements read(ByteBuffer) for
 * Hadoop 1 FSDataInputStream.
 */
class H1SeekableInputStream extends SeekableInputStream {

  private final int COPY_BUFFER_SIZE = 8192;
  private final byte[] temp = new byte[COPY_BUFFER_SIZE];

  private final FSDataInputStream stream;

  public H1SeekableInputStream(FSDataInputStream stream) {
    this.stream = stream;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public long getPos() throws IOException {
    return stream.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    stream.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public void readFully(byte[] bytes) throws IOException {
    stream.readFully(bytes, 0, bytes.length);
  }

  @Override
  public void readFully(byte[] bytes, int start, int len) throws IOException {
    stream.readFully(bytes);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      return readHeapBuffer(stream, buf);
    } else {
      return readDirectBuffer(stream, buf, temp);
    }
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
      readFullyHeapBuffer(stream, buf);
    } else {
      readFullyDirectBuffer(stream, buf, temp);
    }
  }

  // Visible for testing
  static int readHeapBuffer(FSDataInputStream f, ByteBuffer buf) throws IOException {
    int bytesRead = f.read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
    if (bytesRead < 0) {
      // if this resulted in EOF, don't update position
      return bytesRead;
    } else {
      buf.position(buf.position() + bytesRead);
      return bytesRead;
    }
  }

  // Visible for testing
  static void readFullyHeapBuffer(FSDataInputStream f, ByteBuffer buf) throws IOException {
    f.readFully(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
    buf.position(buf.limit());
  }

  // Visible for testing
  static int readDirectBuffer(FSDataInputStream f, ByteBuffer buf, byte[] temp) throws IOException {
    // copy all the bytes that return immediately, stopping at the first
    // read that doesn't return a full buffer.
    int nextReadLength = Math.min(buf.remaining(), temp.length);
    int totalBytesRead = 0;
    int bytesRead;

    while ((bytesRead = f.read(temp, 0, nextReadLength)) == temp.length) {
      buf.put(temp);
      totalBytesRead += bytesRead;
      nextReadLength = Math.min(buf.remaining(), temp.length);
    }

    if (bytesRead < 0) {
      // return -1 if nothing was read
      return totalBytesRead == 0 ? -1 : totalBytesRead;
    } else {
      // copy the last partial buffer
      buf.put(temp, 0, bytesRead);
      totalBytesRead += bytesRead;
      return totalBytesRead;
    }
  }

  // Visible for testing
  static void readFullyDirectBuffer(FSDataInputStream f, ByteBuffer buf, byte[] temp) throws IOException {
    int nextReadLength = Math.min(buf.remaining(), temp.length);
    int bytesRead = 0;

    while (nextReadLength > 0 && (bytesRead = f.read(temp, 0, nextReadLength)) >= 0) {
      buf.put(temp, 0, bytesRead);
      nextReadLength = Math.min(buf.remaining(), temp.length);
    }

    if (bytesRead < 0 && buf.remaining() > 0) {
      throw new EOFException(
          "Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
    }
  }
}
