/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.wrapped.io.DynamicWrappedIO;

/**
 * Class for enhanced FileIO, calling {@link DynamicWrappedIO} as appropriate.
 * If/when Parquet sets a baseline release with the relevant methods directly
 * invocable, this class is where changes would need to be made.
 */
public class HadoopFileIO {

  /**
   * Get the status of file; if the file is not found downgrade to null.
   * @param fileSystem FileSystem to use
   * @param filePath file to load
   * @return the status or null
   * @throws IOException IO failure other than FileNotFoundException
   */
  public static FileStatus getFileStatusOrNull(final FileSystem fileSystem, final Path filePath) throws IOException {
    final FileStatus commonFileStatus;
    try {
      commonFileStatus = fileSystem.getFileStatus(filePath);
    } catch (FileNotFoundException e) {
      // file does not exist
      return null;
    }
    return commonFileStatus;
  }

  /**
   * Open a file for reading using a read policy appropriate for the purpose,
   * passing in a status object containing filename, length and possibly more
   * <p>
   * Note that for filesystems with lazy IO, existence checks may be delayed until
   * the first read() operation.
   *
   * @param fileSystem FileSystem to use
   * @param status file status
   * @param randomIO is the file parquet file to be read with random IO?
   *
   * @return an input stream.
   *
   * @throws IOException failure to open the file.
   */
  public static FSDataInputStream openFile(
      final FileSystem fileSystem, final FileStatus status, final boolean randomIO) throws IOException {
    return DynamicWrappedIO.openFile(fileSystem, status.getPath(), status, readPolicies(randomIO));
  }

  /**
   * Open a file for reading using a read policy appropriate for the purpose.
   * <p>
   * Note that for filesystems with lazy IO, existence checks may be delayed until
   * the first read() operation.
   *
   * @param fileSystem FileSystem to use
   * @param path path to file
   * @param randomIO is the file parquet file to be read with random IO?
   *
   * @return an input stream.
   *
   * @throws IOException failure to open the file.
   */
  public static FSDataInputStream openFile(final FileSystem fileSystem, final Path path, final boolean randomIO)
      throws IOException {
    return DynamicWrappedIO.openFile(fileSystem, path, null, readPolicies(randomIO));
  }

  /**
   * Choose the read policies for the desired purpose.
   * @param randomIO is the file parquet file to be read with random IO?
   * @return appropriate policy string
   */
  private static String readPolicies(final boolean randomIO) {
    return randomIO ? DynamicWrappedIO.PARQUET_READ_POLICIES : DynamicWrappedIO.SEQUENTIAL_READ_POLICIES;
  }
}
