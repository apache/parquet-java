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
package org.apache.parquet.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * {@code LocalInputFile} is an implementation needed by Parquet to read
 * from local data files using {@link SeekableInputStream} instances.
 */
public class LocalInputFile implements InputFile {

  private final Path path;

  public LocalInputFile(Path file) {
    path = file;
  }

  @Override
  public long getLength() throws IOException {
    RandomAccessFile file = new RandomAccessFile(path.toFile(), "r");
    long length = file.length();
    file.close();
    return length;
  }

  @Override
  public SeekableInputStream newStream() throws IOException {

    return new SeekableInputStream() {

      final RandomAccessFile randomAccessFile = new RandomAccessFile(path.toFile(), "r");

      @Override
      public int read() throws IOException {
        return randomAccessFile.read();
      }

      @Override
      public long getPos() throws IOException {
        return randomAccessFile.getFilePointer();
      }

      @Override
      public void seek(long newPos) throws IOException {
        randomAccessFile.seek(newPos);
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        randomAccessFile.readFully(bytes);
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        randomAccessFile.readFully(bytes, start, len);
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[buf.remaining()];
        int code = read(buffer);
        buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining());
        return code;
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[buf.remaining()];
        readFully(buffer);
        buf.put(buffer, buf.position() + buf.arrayOffset(), buf.remaining());
      }

      @Override
      public void close() throws IOException {
        randomAccessFile.close();
      }
    };
  }
}
