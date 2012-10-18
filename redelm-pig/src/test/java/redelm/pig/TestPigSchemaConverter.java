package redelm.pig;

import static org.junit.Assert.assertEquals;
import static redelm.schema.PrimitiveType.Primitive.STRING;
import static redelm.schema.Type.Repetition.OPTIONAL;
import static redelm.schema.Type.Repetition.REPEATED;

import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.junit.Test;

import redelm.schema.GroupType;
import redelm.schema.MessageType;
import redelm.schema.PrimitiveType;

public class TestPigSchemaConverter {

  @Test
  public void testSchemaConversion() throws Exception {
    PigSchemaConverter pigSchemaConverter = new PigSchemaConverter();
    Schema pigSchema = Utils.getSchemaFromString("a:chararray, b:{t:(c:chararray, d:chararray)}");
    MessageType schema = pigSchemaConverter.convert(pigSchema);
    MessageType expected =
        new MessageType("PigSchema",
            new PrimitiveType(OPTIONAL, STRING, "a"),
            new GroupType(REPEATED, "b::t",
                new PrimitiveType(OPTIONAL, STRING, "c"),
                new PrimitiveType(OPTIONAL, STRING, "d")));

    assertEquals(expected, schema);
  }
}
