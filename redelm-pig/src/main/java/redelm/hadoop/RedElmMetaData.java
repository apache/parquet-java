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
package redelm.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

public class RedElmMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  public static class FileMetaData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String schema;
    private final String codecClassName;

    public FileMetaData(String schema, String codecClassName) {
      super();
      this.schema = schema;
      this.codecClassName = codecClassName;
    }

    public String getSchema() {
      return schema;
    }

    public String getCodecClassName() {
      return codecClassName;
    }
  }

  private static final String META_DATA_BLOCK_NAME = "RedElm";

  private static ObjectMapper objectMapper = new ObjectMapper();
  private static ObjectMapper prettyObjectMapper = new ObjectMapper();
  static {
    prettyObjectMapper.getSerializationConfig().set(Feature.INDENT_OUTPUT, true);
  }

  public static String toJSON(RedElmMetaData redElmMetaData) {
    return toJSON(redElmMetaData, objectMapper);
  }

  public static String toPrettyJSON(RedElmMetaData redElmMetaData) {
    return toJSON(redElmMetaData, prettyObjectMapper);
  }

  private static String toJSON(RedElmMetaData redElmMetaData, ObjectMapper mapper) {
    StringWriter stringWriter = new StringWriter();
    try {
      mapper.writeValue(stringWriter, redElmMetaData);
    } catch (JsonGenerationException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return stringWriter.toString();
  }

  public static RedElmMetaData fromJSON(String json) {
    try {
      return objectMapper.readValue(new StringReader(json), RedElmMetaData.class);
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static RedElmMetaData fromMetaDataBlocks(List<MetaDataBlock> metaDataBlocks) {
    for (MetaDataBlock metaDataBlock : metaDataBlocks) {
      if (metaDataBlock.getName().equals(META_DATA_BLOCK_NAME)) {
        try {
          return (RedElmMetaData)new ObjectInputStream(new ByteArrayInputStream(metaDataBlock.getData())).readObject();
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    }
    throw new RuntimeException("There should always be a RedElm metadata block");
  }

  private final FileMetaData fileMetaData;
  private final List<BlockMetaData> blocks;

  public RedElmMetaData(FileMetaData fileMetaData, List<BlockMetaData> blocks) {
    this.fileMetaData = fileMetaData;
    this.blocks = blocks;
  }

  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  public FileMetaData getFileMetaData() {
    return fileMetaData;
  }

  @Override
  public String toString() {
    return "RedElmMetaData{"+fileMetaData+", blocks"+blocks+"}";

  }
}
