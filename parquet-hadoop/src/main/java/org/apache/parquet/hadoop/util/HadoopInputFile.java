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

import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.apache.parquet.hadoop.util.wrapped.io.FutureIO.awaitFuture;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class HadoopInputFile implements InputFile {

  /**
   * openFile() option name for setting the read policy: {@value}.
   */
  private static final String OPENFILE_READ_POLICY_KEY = "fs.option.openfile.read.policy";

  /**
   * Read policy when opening parquet files: {@value}.
   * <p>Policy-aware stores pick the first policy they recognize in the list.
   * everything recognizes "random";
   * "vector" came in with 3.4.0, while "parquet" came with Hadoop 3.4.1
   * parquet means "this is a Parquet file, so be clever about footers, prefetch,
   * and expect vector and/or random IO".
   * <p>In Hadoop 3.4.1, "parquet" and "vector" are both mapped to "random" for the
   * S3A connector, but as the ABFS and GCS connectors do footer caching, they
   * may use it as a hint to say "fetch the footer and keep it in memory"
   */
  private static final String PARQUET_READ_POLICY = "parquet, vector, random, adaptive";

  private final FileSystem fs;
  private final Path path;
  private final long length;
  private final FileStatus stat;
  private final Configuration conf;

  public static HadoopInputFile fromPath(Path path, Configuration conf) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopInputFile(fs, fs.getFileStatus(path), conf);
  }

  /**
   * Creates an input file using a caller-supplied file length.
   *
   * <p>The length is trusted and no file status lookup is performed. Callers must provide the exact
   * length of the file that will be opened. The file system may defer checking whether the file
   * exists until the first read. If the supplied length is incorrect, reads may fail.
   *
   * @param path file path
   * @param length exact file length in bytes
   * @param conf configuration used to resolve the file system
   * @return an input file that uses the supplied length
   * @throws IllegalArgumentException if {@code length} is negative
   * @throws IOException if the file system cannot be resolved
   */
  public static HadoopInputFile fromPath(Path path, long length, Configuration conf) throws IOException {
    if (length < 0) {
      throw new IllegalArgumentException("Invalid file length: " + length);
    }
    FileSystem fs = path.getFileSystem(conf);
    return new HadoopInputFile(fs, path, length, conf);
  }

  public static HadoopInputFile fromPathUnchecked(Path path, Configuration conf) {
    try {
      return fromPath(path, conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf) throws IOException {
    FileSystem fs = stat.getPath().getFileSystem(conf);
    return new HadoopInputFile(fs, stat, conf);
  }

  private HadoopInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.path = stat.getPath();
    this.length = stat.getLen();
    this.stat = stat;
    this.conf = conf;
  }

  private HadoopInputFile(FileSystem fs, Path path, long length, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.length = length;
    this.stat = null;
    this.conf = conf;
  }

  public Configuration getConfiguration() {
    return conf;
  }

  public Path getPath() {
    return path;
  }

  @Override
  public long getLength() {
    return length;
  }

  /**
   * Open the file.
   * <p>Uses {@code FileSystem.openFile()} so that the existing FileStatus, when available, or the
   * known file length can be passed down. File systems may use either to avoid a metadata request
   * when opening the file.
   *
   * @return the input stream.
   *
   * @throws InterruptedIOException future was interrupted
   * @throws IOException if something went wrong
   * @throws RuntimeException any nested RTE thrown
   */
  @Override
  public SeekableInputStream newStream() throws IOException {
    FSDataInputStream stream;
    try {
      // this method is async so that implementations may do async HEAD
      // requests, such as S3A/ABFS when a file status is passed down.
      FutureDataInputStreamBuilder builder = fs.openFile(path).opt(OPENFILE_READ_POLICY_KEY, PARQUET_READ_POLICY);
      if (stat != null) {
        builder.withFileStatus(stat);
      } else {
        builder.opt(FS_OPTION_OPENFILE_LENGTH, Long.toString(length));
      }
      final CompletableFuture<FSDataInputStream> future = builder.build();
      stream = awaitFuture(future);
    } catch (RuntimeException e) {
      // S3A < 3.3.5 would raise illegal path exception if the openFile path didn't
      // equal the path in the FileStatus; Hive virtual FS could create this condition.
      // When a status is supplied, the path to open is derived from stat.getPath(),
      // so this condition seems near-impossible to create -but is handled here
      // for due diligence.
      try {
        stream = fs.open(path);
      } catch (IOException | RuntimeException ex) {
        // failure on this attempt attaches the failure of the openFile() call
        // so the stack trace is preserved.
        ex.addSuppressed(e);
        throw ex;
      }
    }

    return HadoopStreams.wrap(stream);
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
