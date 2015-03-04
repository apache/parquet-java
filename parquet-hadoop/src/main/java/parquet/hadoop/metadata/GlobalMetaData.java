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
package parquet.hadoop.metadata;

import static java.util.Collections.unmodifiableMap;
import static parquet.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import parquet.schema.MessageType;

/**
 * Merged metadata when reading from multiple files.
 * THis is to allow schema evolution
 *
 * @author Julien Le Dem
 *
 */
public class GlobalMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageType schema;

  private final Map<String, Set<String>> keyValueMetaData;

  private final Set<String> createdBy;

  /**
   * @param schema the union of the schemas for all the files
   * @param keyValueMetaData the merged app specific metadata
   * @param createdBy the description of the library that created the file
   */
  public GlobalMetaData(MessageType schema, Map<String, Set<String>> keyValueMetaData, Set<String> createdBy) {
    super();
    this.schema = checkNotNull(schema, "schema");
    this.keyValueMetaData = unmodifiableMap(checkNotNull(keyValueMetaData, "keyValueMetaData"));
    this.createdBy = createdBy;
  }

  /**
   * @return the schema for the file
   */
  public MessageType getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "GlobalMetaData{schema: "+schema+ ", metadata: " + keyValueMetaData + "}";
  }

  /**
   * @return meta data for extensions
   */
  public Map<String, Set<String>> getKeyValueMetaData() {
    return keyValueMetaData;
  }

  /**
   * @return the description of the library that created the file
   */
  public Set<String> getCreatedBy() {
    return createdBy;
  }

  /**
   * Will merge the metadata as if it was coming from a single file.
   * (for all part files written together this will always work)
   * If there are conflicting values an exception will be thrown
   * @return the merged version of this
   */
  public FileMetaData merge() {
    String createdByString = createdBy.size() == 1 ?
      createdBy.iterator().next() :
      createdBy.toString();
    Map<String, String> mergedKeyValues = new HashMap<String, String>();
    for (Entry<String, Set<String>> entry : keyValueMetaData.entrySet()) {
      if (entry.getValue().size() > 1) {
        throw new RuntimeException("could not merge metadata: key " + entry.getKey() + " has conflicting values: " + entry.getValue());
      }
      mergedKeyValues.put(entry.getKey(), entry.getValue().iterator().next());
    }
    return new FileMetaData(schema, mergedKeyValues, createdByString);
  }

}