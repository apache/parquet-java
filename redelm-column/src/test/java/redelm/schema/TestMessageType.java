package redelm.schema;

import junit.framework.Assert;
import redelm.data.simple.example.Paper;

import org.junit.Test;

public class TestMessageType {
  @Test
  public void test() {
    System.out.println(Paper.schema.toString());
    MessageType schema = MessageType.parse(Paper.schema.toString());
    Assert.assertEquals(schema.toString(), Paper.schema.toString());
  }
}
