package parquet.hadoop.thrift.struct;

import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;

class JSON {

  private static ObjectMapper om = new ObjectMapper();
  static {
    om.configure(Feature.INDENT_OUTPUT, true);
  }

  static <T> T fromJSON(String json, Class<T> clzz) {
    try {
      return om.readValue(json, clzz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static String toJSON(Object o) {
    final StringWriter sw = new StringWriter();
    try {
      om.writeValue(sw, o);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sw.toString();
  }
}
