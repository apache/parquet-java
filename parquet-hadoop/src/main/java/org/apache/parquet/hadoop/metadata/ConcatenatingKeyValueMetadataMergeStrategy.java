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
package org.apache.parquet.hadoop.metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  Strategy to concatenate if there are multiple values for a given key in metadata.
 *  Note: use this with caution. This is expected to work only for certain use cases.
 */
public class ConcatenatingKeyValueMetadataMergeStrategy implements KeyValueMetadataMergeStrategy {
  private static final String DEFAULT_DELIMITER = ",";
  
  private final String delimiter;

  /**
   * Default constructor.
   */
  public ConcatenatingKeyValueMetadataMergeStrategy() {
    this.delimiter = DEFAULT_DELIMITER;
  }

  /**
   * Constructor to use different delimiter for concatenation.
   * 
   * @param delim delimiter char sequence.
   */
  public ConcatenatingKeyValueMetadataMergeStrategy(String delim) {
    this.delimiter = delim;
  }
  
  /**
   * @param keyValueMetaData the merged app specific metadata
   */
  public Map<String, String> merge(Map<String, Set<String>> keyValueMetaData) {
    Map<String, String> mergedKeyValues = new HashMap<String, String>();
    for (Map.Entry<String, Set<String>> entry : keyValueMetaData.entrySet()) {
      mergedKeyValues.put(entry.getKey(), entry.getValue().stream().collect(Collectors.joining(this.delimiter)));
    }
    return mergedKeyValues;
  }
}
