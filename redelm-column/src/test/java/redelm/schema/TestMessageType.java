package redelm.schema;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import redelm.data.simple.example.Paper;
import redelm.parser.RedelmParser;

public class TestMessageType {
  @Test
  public void test() throws Exception {
    System.out.println(Paper.schema.toString());
    MessageType schema = RedelmParser.parse(Paper.schema.toString());
    assertEquals(Paper.schema, schema);
    assertEquals(schema.toString(), Paper.schema.toString());
  }
}
