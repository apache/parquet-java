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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HadoopOutputFile implements OutputFile {
  // need to supply a buffer size when setting block size. this is the default
  // for hadoop 1 to present. copying it avoids loading DFSConfigKeys.
  private static final int DFS_BUFFER_SIZE_DEFAULT = 4096;

  private static final Set<String> BLOCK_FS_SCHEMES = new HashSet<String>();
  static {
    BLOCK_FS_SCHEMES.add("hdfs");
    BLOCK_FS_SCHEMES.add("webhdfs");
    BLOCK_FS_SCHEMES.add("viewfs");
  }

  // visible for testing
  public static Set<String> getBlockFileSystems() {
    return BLOCK_FS_SCHEMES;
  }

  private static boolean supportsBlockSize(FileSystem fs) {
    return BLOCK_FS_SCHEMES.contains(fs.getUri().getScheme());
  }

  private final FileSystem fs;
  private final Path path;
  private final Configuration conf;

  public static HadoopOutputFile fromPath(Path path, Configuration conf)
      throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopOutputFile(fs, fs.makeQualified(path), conf);
  }

  private HadoopOutputFile(FileSystem fs, Path path, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    return HadoopStreams.wrap(fs.create(path, false /* do not overwrite */,
        DFS_BUFFER_SIZE_DEFAULT, fs.getDefaultReplication(path),
        Math.max(fs.getDefaultBlockSize(path), blockSizeHint)));
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return HadoopStreams.wrap(fs.create(path, true /* overwrite if exists */,
        DFS_BUFFER_SIZE_DEFAULT, fs.getDefaultReplication(path),
        Math.max(fs.getDefaultBlockSize(path), blockSizeHint)));
  }

  @Override
  public boolean supportsBlockSize() {
    return supportsBlockSize(fs);
  }

  @Override
  public long defaultBlockSize() {
    return fs.getDefaultBlockSize(path);
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
