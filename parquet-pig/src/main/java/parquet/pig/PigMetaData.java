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
package parquet.pig;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

public class PigMetaData {

  private static final String PIG_SCHEMA = "pig.schema";

  public static PigMetaData fromMetaData(Map<String, String> keyValueMetaData) {
    if (keyValueMetaData.containsKey(PIG_SCHEMA)) {
      return new PigMetaData(keyValueMetaData.get(PIG_SCHEMA));
    }
    return null;
  }

  private String pigSchema;

  public PigMetaData(String pigSchema) {
    this.pigSchema = pigSchema;
  }

  public void setPigSchema(String pigSchema) {
    this.pigSchema = pigSchema;
  }

  public String getPigSchema() {
    return pigSchema;
  }

  public void addToMetaData(Map<String, String> map) {
    map.put(PIG_SCHEMA, pigSchema);
  }

}
