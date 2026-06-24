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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * An {@link OutputFile} that captures all written data into an in-memory byte array.
 * Useful for producing Parquet files that can later be read via {@link InMemoryInputFile}
 * without touching the filesystem.
 */
public final class InMemoryOutputFile implements OutputFile {

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  /**
   * Returns the captured data as a byte array. Should only be called after the
   * writer has been closed.
   */
  public byte[] toByteArray() {
    return baos.toByteArray();
  }

  /** Returns the current size of the captured data in bytes. */
  public int size() {
    return baos.size();
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return newStream();
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return newStream();
  }

  private PositionOutputStream newStream() {
    return new PositionOutputStream() {
      private long pos = 0;

      @Override
      public void write(int b) throws IOException {
        baos.write(b);
        pos++;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        baos.write(b, off, len);
        pos += len;
      }

      @Override
      public long getPos() {
        return pos;
      }
    };
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  @Override
  public String getPath() {
    return "memory";
  }
}
