package parquet.pig;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.Assert;
import org.junit.Test;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

public class TestTupleConversion {
  @Test
  public void testSimple() throws Exception {
    String parquetSchemaStr = "message FilterListAsBag {\n" +
        "  optional group my_list (LIST) {\n" +
        "    repeated group array {\n" +
        "      optional group array_element {\n" +
        "        required int32 num1;\n" +
        "        required int32 num2;\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n";

    PigSchemaConverter converter = new PigSchemaConverter();

    MessageType parquetSchema = MessageTypeParser.parseMessageType(parquetSchemaStr);
    Schema pigSchema = converter.convert(parquetSchema);

    String intermediatePigSchemaStr = PigSchemaConverter.pigSchemaToString(pigSchema);
    Schema reparsedSchema = PigSchemaConverter.parsePigSchema(intermediatePigSchemaStr);

    MessageType projectedSchema = converter.filter(parquetSchema, reparsedSchema);

    MessageTypeParser.parseMessageType(projectedSchema.toString());
    Assert.assertEquals(parquetSchema, projectedSchema);
  }
}
