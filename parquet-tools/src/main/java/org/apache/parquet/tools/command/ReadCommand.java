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
package org.apache.parquet.tools.command;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

abstract class ReadCommand extends ArgsOnlyCommand {
  private Configuration conf = new Configuration();

  ReadCommand(int min, int max) {
    super(min, max);
  }

  /**
   * Get Parquet metadata for given base path and filter. Path can be a directory, see
   * `mergeFooters` method on how metadata is selected.
   */
  public ParquetMetadata getMetadata(Path basePath, PathFilter filter) throws IOException {
    FileSystem fs = basePath.getFileSystem(conf);
    // Sometimes input path can be a directory, e.g. in case of Spark output, we need to make sure
    // that at least one path exists for extracting metadata
    FileStatus[] statuses = fs.listStatus(basePath, filter);
    if (statuses == null || statuses.length == 0) {
      throw new IOException("No statuses found for path " + basePath);
    }

    List<Footer> footers =
      ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, Arrays.asList(statuses));
    return extractMetadata(footers, basePath);
  }

  /**
   * Extract metadata from list of footers.
   */
  public ParquetMetadata extractMetadata(List<Footer> footers, Path basePath) throws IOException {
    if (basePath == null) {
      throw new IOException("Invalid basePath " + basePath);
    }

    if (footers == null || footers.isEmpty()) {
      throw new IOException("Could not find metadata in " + basePath);
    }

    // Just take the first metadata for now
    return footers.get(0).getParquetMetadata();
  }

  /**
   * Get filter for partition files when base path is a directory, this is used mainly to filter
   * out Spark files.
   */
  public PathFilter filterPartitionFiles() {
    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        // Ignore Spark '_SUCCESS' files
        return path != null && !path.getName().equals("_SUCCESS");
      }
    };
  }
}
