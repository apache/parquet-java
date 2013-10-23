package parquet.proto;

import org.apache.hadoop.util.IndexedSortable;
import org.junit.Test;
import parquet.protobuf.test.TestProtobuf;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class BugHuntingTest {

  @Test
  public void testIndexes() throws Exception {
    TestProtobuf.Indexes.Builder indexes = TestProtobuf.Indexes.newBuilder();
    indexes.setField1("field1");
    indexes.setField2("field2");
    indexes.setField4("field4");
    indexes.setField5("field5");

    List<TestProtobuf.IndexesOrBuilder> results;
    results = TestUtils.writeAndRead((TestProtobuf.IndexesOrBuilder) indexes);

    TestProtobuf.IndexesOrBuilder result = results.get(0);

    assertEquals("field1", result.getField1());
    assertEquals("field2", result.getField2());
    assertEquals("field4", result.getField4());
    assertEquals("field5", result.getField5());
  }

  @Test
  public void testIntArray() throws Exception {
    TestProtobuf.IntArrayMessage.Builder a = TestProtobuf.IntArrayMessage.newBuilder();
    a.addIntArray(123).addIntArray(23).setSecondField("SecondField");

    List<TestProtobuf.IntArrayMessageOrBuilder> result = TestUtils.writeAndRead((TestProtobuf.IntArrayMessageOrBuilder) a);

    List<Integer> intArrayList = result.get(0).getIntArrayList();
    assertEquals(123, (int) intArrayList.get(0));
    assertEquals(23, (int) intArrayList.get(1));
  }

  @Test
  public void testMessageArray() throws Exception {
    TestProtobuf.MessageArrayMessage.Builder a = TestProtobuf.MessageArrayMessage .newBuilder();
    a.addMsgArrayBuilder().setInnerValue("test").setSecondField("SecondFieldInner");
    a.setSecondField("SecondFieldOuter");
    List<TestProtobuf.MessageArrayMessageOrBuilder> result = TestUtils.writeAndRead((TestProtobuf.MessageArrayMessageOrBuilder) a);

    assertEquals("test", result.get(0).getMsgArrayList().get(0).getInnerValue());
  }
  }

