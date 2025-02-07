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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Based on {@code org.apache.hadoop.fs.FileSystemTestHelper},
 * This class exports the package private {@code FileSystem}
 * methods which can be used to push FS instances into the
 * map of URI -> fs instance.
 * <p>
 * This makes it easy to add instances of Mocked filesystems
 * to the map, which will then be picked up by any
 * code retrieving an FS instance for that URI
 * <p>
 * The API is stable and used elsewhere. What is important
 * is to remove FS instances after each test case.
 * {@link #cleanFilesystemCache()} cleans the entire cache
 * and should be used in teardown methods.
 */
public final class FileSystemTestBinder {

  /**
   * Empty configuration.
   * Part of the FileSystem method signatures, but not used behind them.
   */
  public static final Configuration CONF = new Configuration(false);

  /**
   * Inject a filesystem into the cache.
   * @param uri filesystem URI
   * @param fs filesystem to inject
   * @throws UncheckedIOException Hadoop UGI problems.
   */
  public static void addFileSystemForTesting(URI uri, FileSystem fs) {
    try {
      FileSystem.addFileSystemForTesting(uri, CONF, fs);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Clean up the filesystem cache.
   * This swallows any IOE nominally raised in the process, to ensure
   * this can safely invoked in teardowns.
   */
  public static void cleanFilesystemCache() {
    try {
      FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    } catch (IOException ignored) {
      // Ignore the exception as if getCurrentUser() fails then it'll
      // have been impossible to add mock instances to a per-user cache.
    }
  }
}
