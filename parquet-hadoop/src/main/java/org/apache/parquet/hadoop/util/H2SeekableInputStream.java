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

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.SeekableInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * SeekableInputStream implementation for FSDataInputStream that implements
 * ByteBufferReadable in Hadoop 2.
 */
class H2SeekableInputStream extends SeekableInputStream {

  private final FSDataInputStream stream;

  public H2SeekableInputStream(FSDataInputStream stream) {
    Preconditions.checkArgument(stream instanceof ByteBufferReadable,
        "Input stream must be ByteBufferReadable. Try using H1SeekableInputStream");
    this.stream = stream;
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
    return stream.read(buf);
  }

  @Override
  public void readFully(ByteBuffer buf) throws IOException {
    readFully(stream, buf);
  }

  public static void readFully(FSDataInputStream stream, ByteBuffer buf) throws IOException {
    // unfortunately the Hadoop APIs seem to not have a 'readFully' equivalent for the byteBuffer read
    // calls. The read(ByteBuffer) call might read fewer than byteBuffer.hasRemaining() bytes. Thus we
    // have to loop to ensure we read them.
    while (buf.hasRemaining()) {
      int readCount = stream.read(buf);
      if (readCount == -1) {
        // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
        // that has more remaining than the amount of data in the stream.
        throw new EOFException("Reached the end of stream. Still have: " + buf.remaining() + " bytes left");
      }
    }
  }
}
