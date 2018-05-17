package parquet.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static parquet.io.api.Binary.fromString;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.Type.Repetition.OPTIONAL;
import static parquet.schema.Type.Repetition.REPEATED;
import static parquet.schema.Type.Repetition.REQUIRED;

import org.junit.Test;

import parquet.data.materializer.GroupBuilderImpl;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

public class TestGroupBuilder {

  @Test
  public void test() {
    MessageType schema = new MessageType("Foo",
        new PrimitiveType(REQUIRED, INT32, "int"),
        new PrimitiveType(REQUIRED, BINARY, "bin"),
        new PrimitiveType(REPEATED, BINARY, "bin2"),
        new GroupType(OPTIONAL, "bar",
            new PrimitiveType(REQUIRED, INT32, "int")));
    GroupBuilder gb = GroupBuilderImpl.newGroupBuilderImpl(schema);
    for (int i = 0; i < 10; i++) {
      GroupBuilder cgb = gb.startMessage();
      cgb = cgb.addIntValue("int", i)
          .addBinaryValue("bin", fromString("bin " + i));
      for (int j = 0; j < i; j++) {
        cgb = cgb.addBinaryValue("bin2", fromString("bin2 " + j));
      }
      if ( i % 2 == 0) {
        cgb = cgb.startGroup("bar").addIntValue("int", i).endGroup();
      }
      Group g = cgb.endMessage();
      assertEquals(i, g.getInt("int"));
      assertEquals("bin " + i, g.getBinary("bin").toStringUsingUTF8());
      assertEquals(i, g.getRepetitionCount("bin2"));
      for (int j = 0; j < g.getRepetitionCount("bin2"); ++j) {
        assertEquals("bin2 " + j, g.getRepeatedBinaryAt("bin2", j).toStringUsingUTF8());
      }
      if ( i % 2 == 0) {
        assertTrue(g.isDefined("bar"));
        assertEquals(i, g.getGroup("bar").getInt("int"));
      } else {
        assertFalse(g.isDefined("bar"));
      }
    }
  }
}
