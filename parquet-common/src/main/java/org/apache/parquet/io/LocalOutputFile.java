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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * {@code LocalOutputFile} is an implementation needed by Parquet to write
 * to local data files using {@link PositionOutputStream} instances.
 */
public class LocalOutputFile implements OutputFile {

  private static final int BUFFER_SIZE_DEFAULT = 4096;

  private class LocalPositionOutputStream extends PositionOutputStream {

    private final BufferedOutputStream stream;
    private long pos = 0;

    public LocalPositionOutputStream(int buffer, StandardOpenOption... openOption) throws IOException {
      stream = new BufferedOutputStream(Files.newOutputStream(path, openOption), buffer);
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int data) throws IOException {
      pos++;
      stream.write(data);
    }

    @Override
    public void write(byte[] data) throws IOException {
      pos += data.length;
      stream.write(data);
    }

    @Override
    public void write(byte[] data, int off, int len) throws IOException {
      pos += len;
      stream.write(data, off, len);
    }

    @Override
    public void flush() throws IOException {
      stream.flush();
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  private final Path path;

  public LocalOutputFile(Path file) {
    path = file;
  }

  @Override
  public PositionOutputStream create(long blockSize) throws IOException {
    return new LocalPositionOutputStream(BUFFER_SIZE_DEFAULT, StandardOpenOption.CREATE_NEW);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSize) throws IOException {
    return new LocalPositionOutputStream(
        BUFFER_SIZE_DEFAULT, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return -1;
  }

  @Override
  public String getPath() {
    return path.toString();
  }
}
