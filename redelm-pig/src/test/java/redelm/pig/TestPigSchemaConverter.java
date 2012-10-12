package redelm.pig;

import junit.framework.Assert;
import redelm.schema.MessageType;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

public class TestPigSchemaConverter {

  @Test
  public void test() throws Exception {
    PigSchemaConverter pigSchemaConverter = new PigSchemaConverter();
    Schema pigSchema = Utils.getSchemaFromString("a:chararray, b:{t:(c:chararray, d:chararray)}");
    MessageType schema = pigSchemaConverter.convert(pigSchema);
    String expected =
    "message message {\n" +
    "  optional string a;\n" +
    "  repeated group b_t {\n" +
    "    optional string c;\n" +
    "    optional string d;\n" +
    "  };\n" +
    "}\n";
    Assert.assertEquals(expected, schema.toString());
  }
}
