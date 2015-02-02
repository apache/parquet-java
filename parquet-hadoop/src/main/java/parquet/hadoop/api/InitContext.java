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
package parquet.hadoop.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import parquet.schema.MessageType;

/**
 *
 * Context passed to ReadSupport when initializing for read
 *
 * @author Julien Le Dem
 *
 */
public class InitContext {

  private final Map<String,Set<String>> keyValueMetadata;
  private Map<String,String> mergedKeyValueMetadata;
  private final Configuration configuration;
  private final MessageType fileSchema;

  /**
   * @param configuration the hadoop configuration
   * @param keyValueMetadata extra metadata from file footers
   * @param fileSchema the merged schema from the files
   */
  public InitContext(
      Configuration configuration,
      Map<String, Set<String>> keyValueMetadata,
      MessageType fileSchema) {
    super();
    this.keyValueMetadata = keyValueMetadata;
    this.configuration = configuration;
    this.fileSchema = fileSchema;
  }

  /**
   * If there is a conflicting value when reading from multiple files,
   * an exception will be thrown
   * @return the merged key values metadata form the file footers
   */
  @Deprecated
  public Map<String, String> getMergedKeyValueMetaData() {
    if (mergedKeyValueMetadata == null) {
      Map<String, String> mergedKeyValues = new HashMap<String, String>();
      for (Entry<String, Set<String>> entry : keyValueMetadata.entrySet()) {
        if (entry.getValue().size() > 1) {
          throw new RuntimeException("could not merge metadata: key " + entry.getKey() + " has conflicting values: " + entry.getValue());
        }
        mergedKeyValues.put(entry.getKey(), entry.getValue().iterator().next());
      }
      mergedKeyValueMetadata = mergedKeyValues;
    }
    return mergedKeyValueMetadata;
  }

  /**
   * @return the configuration for this job
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * this is the union of all the schemas when reading multiple files.
   * @return the schema of the files being read
   */
  public MessageType getFileSchema() {
    return fileSchema;
  }

  /**
   * each key is associated with the list of distinct values found in footers
   * @return the merged metadata from the footer of the file
   */
  public Map<String, Set<String>> getKeyValueMetadata() {
    return keyValueMetadata;
  }

}
