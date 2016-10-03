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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.InputFile;
import java.io.IOException;

public class HadoopInputFile implements InputFile {

  private final FileSystem fs;
  private final FileStatus stat;

  public static HadoopInputFile fromPath(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopInputFile(fs, fs.getFileStatus(path));
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf)
      throws IOException {
    FileSystem fs = stat.getPath().getFileSystem(conf);
    return new HadoopInputFile(fs, stat);
  }

  private HadoopInputFile(FileSystem fs, FileStatus stat) {
    this.fs = fs;
    this.stat = stat;
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return HadoopStreams.wrap(fs.open(stat.getPath()));
  }

  @Override
  public String toString() {
    return stat.getPath().toString();
  }
}
