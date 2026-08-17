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
package org.apache.parquet.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * Represent the footer for a given file
 */
public class Footer {

  private final Path file;

  private final ParquetMetadata parquetMetadata;

  private final FileStatus fileStatus;

  /**
   * Constructor for backwards compatibility
   *
   * @param file the file path
   * @param parquetMetadata the parquet metadata
   */
  public Footer(Path file, ParquetMetadata parquetMetadata) {
    this(file, parquetMetadata, null);
  }

  /**
   * Constructor with FileStatus to avoid redundant getFileStatus RPC calls
   *
   * @param file the file path
   * @param parquetMetadata the parquet metadata
   * @param fileStatus the file status (may be null for backwards compatibility)
   */
  public Footer(Path file, ParquetMetadata parquetMetadata, FileStatus fileStatus) {
    super();
    this.file = file;
    this.parquetMetadata = parquetMetadata;
    this.fileStatus = fileStatus;
  }

  public Path getFile() {
    return file;
  }

  public ParquetMetadata getParquetMetadata() {
    return parquetMetadata;
  }

  /**
   * Get the FileStatus associated with this footer.
   * This is used to avoid redundant getFileStatus RPC calls to the NameNode.
   *
   * @return the FileStatus, or null if not available
   */
  public FileStatus getFileStatus() {
    return fileStatus;
  }

  @Override
  public String toString() {
    return "Footer{" + file + ", " + parquetMetadata + "}";
  }
}
