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

package org.apache.parquet.io;

import java.io.IOException;

/**
 * An {@link OutputFile} implementation that allows Parquet to write to {@link System#out}
 */
public class StandardOutputFile implements OutputFile {

  /**
   * @implNote we don't want to close the standard output (it's up to the process lifecycle to handle that)
   */
  public class RawPositionOutputStream extends PositionOutputStream {

    private long pos = 0;

    @Override
    public long getPos() throws IOException {
      return this.pos;
    }

    @Override
    public void write(int b) throws IOException {
      this.pos++;
      System.out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      this.pos += b.length;
      System.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      this.pos += len;
      System.out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      System.out.flush();
    }
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return new RawPositionOutputStream();
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new RawPositionOutputStream();
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return -1;
  }
}
