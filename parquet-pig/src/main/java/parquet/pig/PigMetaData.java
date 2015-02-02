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
package parquet.pig;

import static parquet.pig.PigSchemaConverter.pigSchemaToString;

import java.util.Map;
import java.util.Set;

import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Represents Pig meta data stored in the file footer
 *
 * @author Julien Le Dem
 *
 */
public class PigMetaData {

  private static final String PIG_SCHEMA = "pig.schema";

  /**
   * @param keyValueMetaData the key values from the footer
   * @return the parsed Pig metadata
   */
  public static PigMetaData fromMetaData(Map<String, String> keyValueMetaData) {
    if (keyValueMetaData.containsKey(PIG_SCHEMA)) {
      return new PigMetaData(keyValueMetaData.get(PIG_SCHEMA));
    }
    return null;
  }

  /**
   * @param keyValueMetaData the key values from the footers
   * @return the list pig schemas from the footers
   */
  public static Set<String> getPigSchemas(Map<String, Set<String>> keyValueMetaData) {
    return keyValueMetaData.get(PIG_SCHEMA);
  }

  private String pigSchema;

  /**
   * @param pigSchema the pig schema of the file
   */
  public PigMetaData(Schema pigSchema) {
    this.pigSchema = pigSchemaToString(pigSchema);
  }

  /**
   * @param pigSchema the pig schema of the file
   */
  public PigMetaData(String pigSchema) {
    this.pigSchema = pigSchema;
  }

  /**
   * @param pigSchema the pig schema of the file
   */
  public void setPigSchema(String pigSchema) {
    this.pigSchema = pigSchema;
  }

  /**
   * @return the pig schema of the file
   */
  public String getPigSchema() {
    return pigSchema;
  }

  /**
   * @param map where to add the key values representing this metadata
   */
  public void addToMetaData(Map<String, String> map) {
    map.put(PIG_SCHEMA, pigSchema);
  }

}
