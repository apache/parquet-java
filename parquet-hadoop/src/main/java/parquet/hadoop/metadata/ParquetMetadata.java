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
package parquet.hadoop.metadata;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

/**
 * Meta Data block stored in the footer of the file
 * contains file level (Codec, Schema, ...) and block level (location, columns, record count, ...) meta data
 *
 * @author Julien Le Dem
 *
 */
public class ParquetMetadata {

  private static ObjectMapper objectMapper = new ObjectMapper();
  private static ObjectMapper prettyObjectMapper = new ObjectMapper();
  static {
    prettyObjectMapper.configure(Feature.INDENT_OUTPUT, true);
  }

  /**
   *
   * @param parquetMetaData
   * @return the json representation
   */
  public static String toJSON(ParquetMetadata parquetMetaData) {
    return toJSON(parquetMetaData, objectMapper);
  }

  /**
   *
   * @param parquetMetaData
   * @return the pretty printed json representation
   */
  public static String toPrettyJSON(ParquetMetadata parquetMetaData) {
    return toJSON(parquetMetaData, prettyObjectMapper);
  }

  private static String toJSON(ParquetMetadata parquetMetaData, ObjectMapper mapper) {
    StringWriter stringWriter = new StringWriter();
    try {
      mapper.writeValue(stringWriter, parquetMetaData);
    } catch (JsonGenerationException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return stringWriter.toString();
  }

  /**
   *
   * @param json the json representation
   * @return the parsed object
   */
  public static ParquetMetadata fromJSON(String json) {
    try {
      return objectMapper.readValue(new StringReader(json), ParquetMetadata.class);
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final FileMetaData fileMetaData;
  private final List<BlockMetaData> blocks;
  private final Map<String, String> keyValueMetaData;

  /**
   *
   * @param fileMetaData file level metadata
   * @param blocks block level metadata
   * @param keyValueMetaData
   */
  public ParquetMetadata(FileMetaData fileMetaData, List<BlockMetaData> blocks, Map<String, String> keyValueMetaData) {
    this.fileMetaData = fileMetaData;
    this.blocks = blocks;
    this.keyValueMetaData = keyValueMetaData;
  }

  /**
   *
   * @return block level metadata
   */
  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  /**
   *
   * @return file level meta data
   */
  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  /**
   *
   * @return meta data for extensions
   */
  public Map<String, String> getKeyValueMetaData() {
    return keyValueMetaData;
  }

  @Override
  public String toString() {
    return "ParquetMetaData{"+fileMetaData+", blocks: "+blocks+"}";
  }

}
