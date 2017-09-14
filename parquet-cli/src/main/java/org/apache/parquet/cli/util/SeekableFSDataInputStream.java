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

package org.apache.parquet.cli.util;

import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.InputStream;

/**
 * A wrapper for FSDataInputStream that implements Avro's SeekableInput.
 */
public class SeekableFSDataInputStream extends InputStream implements SeekableInput {
  private final FSDataInputStream in;
  private final FileStatus stat;

  public SeekableFSDataInputStream(FileSystem fs, Path file) throws IOException {
    this.in = fs.open(file);
    this.stat = fs.getFileStatus(file);
  }

  @Override
  public void seek(long p) throws IOException {
    in.seek(p);
  }

  @Override
  public long tell() throws IOException {
    return in.getPos();
  }

  @Override
  public long length() throws IOException {
    return stat.getLen();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
