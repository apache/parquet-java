package redelm.pig;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;

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

  public static PigMetaData fromMetaDataBlocks(List<MetaDataBlock> metaDataBlocks) {
    for (MetaDataBlock metaDataBlock : metaDataBlocks) {
      if (metaDataBlock.getName().equals(META_DATA_BLOCK_NAME)) {
        return fromJSON(new String(metaDataBlock.getData(), Charset.forName("UTF-8")));
      }
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

  public MetaDataBlock toMetaDataBlock() {
   return new MetaDataBlock(META_DATA_BLOCK_NAME, toJSON().getBytes(Charset.forName("UTF-8")));
  }

}
