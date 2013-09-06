/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import parquet.schema.MessageType;

public class InitContext {

  private final Map<String,Set<String>> keyValueMetadata;
  private Map<String,String> mergedKeyValueMetadata;
  private final Configuration configuration;
  private final MessageType fileSchema;

  public InitContext(
      Configuration configuration,
      Map<String, Set<String>> keyValueMetadata,
      MessageType fileSchema) {
    super();
    this.keyValueMetadata = keyValueMetadata;
    this.configuration = configuration;
    this.fileSchema = fileSchema;
  }

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

  @Deprecated
  public Configuration getConfiguration() {
    return configuration;
  }

  public MessageType getFileSchema() {
    return fileSchema;
  }

  public Map<String, Set<String>> getKeyValueMetadata() {
    return keyValueMetadata;
  }

}
