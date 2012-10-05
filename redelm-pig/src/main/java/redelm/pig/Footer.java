package redelm.pig;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

public class Footer implements Serializable {
  private static final long serialVersionUID = 1L;

  private static ObjectMapper objectMapper = new ObjectMapper();
  private static ObjectMapper prettyObjectMapper = new ObjectMapper();
  static {
    prettyObjectMapper.getSerializationConfig().set(Feature.INDENT_OUTPUT, true);
  }

  public static String toJSON(Footer footer) {
    return toJSON(footer, objectMapper);
  }

  public static String toPrettyJSON(Footer footer) {
    return toJSON(footer, prettyObjectMapper);
  }

  private static String toJSON(Footer footer, ObjectMapper mapper) {
    StringWriter stringWriter = new StringWriter();
    try {
      mapper.writeValue(stringWriter, footer);
    } catch (JsonGenerationException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return stringWriter.toString();
  }

  public static Footer fromJSON(String json) {
    try {
      return objectMapper.readValue(new StringReader(json), Footer.class);
    } catch (JsonParseException e) {
      throw new RuntimeException(e);
    } catch (JsonMappingException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final String schema;
  private final List<BlockMetaData> blocks;

  public Footer(String schema, List<BlockMetaData> blocks) {
    this.schema = schema;
    this.blocks = blocks;
  }

  public List<BlockMetaData> getBlocks() {
    return blocks;
  }

  public String getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return "Footer{"+blocks+"}";
  }
}
