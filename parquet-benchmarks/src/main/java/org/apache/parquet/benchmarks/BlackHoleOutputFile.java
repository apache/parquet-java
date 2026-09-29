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

import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 * A no-op {@link OutputFile} that discards all written data.
 * Useful for isolating CPU/encoding cost from filesystem I/O in write benchmarks.
 */
public final class BlackHoleOutputFile implements OutputFile {

  public static final BlackHoleOutputFile INSTANCE = new BlackHoleOutputFile();

  private BlackHoleOutputFile() {}

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return -1L;
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    return create(blockSizeHint);
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new PositionOutputStream() {
      private long pos;

      @Override
      public long getPos() throws IOException {
        return pos;
      }

      @Override
      public void write(int b) throws IOException {
        ++pos;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        pos += len;
      }
    };
  }

  @Override
  public String getPath() {
    return "/dev/null";
  }
}
