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

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class TestColumnConfigParser {

  @Test
  public void structuredColumnPathsRoundTripWithoutCollisions() {
    String rootKey = "parquet.test";
    String[][] paths = {{"a.b"}, {"a", "b"}, {""}, {"münchen", "street.name"}};
    Configuration conf = new Configuration(false);
    for (int i = 0; i < paths.length; i++) {
      conf.setInt(ColumnConfigParser.columnPathKey(rootKey, paths[i]), i);
    }

    Map<Integer, String[]> parsedPaths = new HashMap<>();
    new ColumnConfigParser()
        .withColumnPathConfig(
            rootKey, key -> conf.getInt(key, -1), (path, value) -> parsedPaths.put(value, path))
        .parseConfig(conf);

    assertThat(parsedPaths).hasSize(paths.length);
    assertThat(parsedPaths.get(0)).containsExactly("a.b");
    assertThat(parsedPaths.get(1)).containsExactly("a", "b");
    assertThat(parsedPaths.get(2)).containsExactly("");
    assertThat(parsedPaths.get(3)).containsExactly("münchen", "street.name");
  }
}
