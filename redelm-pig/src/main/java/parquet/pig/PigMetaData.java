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

  private static final String META_DATA_BLOCK_NAME = "Pig";
  private static ObjectMapper objectMapper = new ObjectMapper();
  private static ObjectMapper prettyObjectMapper = new ObjectMapper();
  static {
    prettyObjectMapper.getSerializationConfig().set(Feature.INDENT_OUTPUT, true);
  }

  public String toJSON() {
    return toJSON(this, objectMapper);
  }

  public String toPrettyJSON() {
    return toJSON(this, prettyObjectMapper);
  }

  private static String toJSON(PigMetaData metaData, ObjectMapper mapper) {
    StringWriter stringWriter = new StringWriter();
    try {
      mapper.writeValue(stringWriter, metaData);
    } catch (JsonGenerationException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return stringWriter.toString();
  }

  public static PigMetaData fromJSON(String json) {
    try {
      return objectMapper.readValue(new StringReader(json), PigMetaData.class);
    } catch (JsonParseException e) {
      throw new RuntimeException(json, e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(json, e);
    } catch (IOException e) {
      throw new RuntimeException(json, e);
    }
  }

  public static PigMetaData fromMetaDataBlocks(Map<String, String> keyValueMetaData) {
    if (keyValueMetaData.containsKey(META_DATA_BLOCK_NAME)) {
      return fromJSON(keyValueMetaData.get(META_DATA_BLOCK_NAME));
    }
    return null;
  }

  private String pigSchema;

  public PigMetaData() {
  }

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
    map.put(META_DATA_BLOCK_NAME, toJSON());
  }

}
