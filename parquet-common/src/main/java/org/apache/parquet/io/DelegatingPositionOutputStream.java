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
import java.io.OutputStream;

public abstract class DelegatingPositionOutputStream extends PositionOutputStream {
  private final OutputStream stream;

  public DelegatingPositionOutputStream(OutputStream stream) {
    this.stream = stream;
  }

  public OutputStream getStream() {
    return stream;
  }

  @Override
  public void close() throws IOException {
    try (OutputStream os = this.stream) {
      os.flush();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public abstract long getPos() throws IOException;

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
  }
}
