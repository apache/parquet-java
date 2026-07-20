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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.Statistics;

public class TestUtils {

  public static void enforceEmptyDir(Configuration conf, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      if (!fs.delete(path, true)) {
        throw new IOException("can not delete path " + path);
      }
    }
    if (!fs.mkdirs(path)) {
      throw new IOException("can not create path " + path);
    }
  }

  public static void assertStatsValuesEqual(Statistics<?> stats1, Statistics<?> stats2) {
    assertStatsValuesEqual(null, stats1, stats2);
  }

  // To be used to assert that the values (min, max, num-of-nulls) equals. It might be used in cases when creating
  // Statistics object for the proper Type would require too much work/code duplications etc.
  public static void assertStatsValuesEqual(String message, Statistics<?> expected, Statistics<?> actual) {
    if (expected == actual) {
      return;
    }
    if (expected == null || actual == null) {
      assertThat(actual).isEqualTo(expected);
      return;
    }
    assertThat(actual).as(message).isInstanceOf(expected.getClass());
    assertThat(actual.getMaxBytes()).as(message).isEqualTo(expected.getMaxBytes());
    assertThat(actual.getMinBytes()).as(message).isEqualTo(expected.getMinBytes());
    assertThat(actual.getNumNulls()).as(message).isEqualTo(expected.getNumNulls());
  }
}
