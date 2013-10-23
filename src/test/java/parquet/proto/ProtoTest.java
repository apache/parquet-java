package parquet.proto;

import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.protobuf.test.TestProtobuf;

import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static parquet.proto.TestUtils.*;
import static parquet.protobuf.test.TestProtobuf.*;

public class ProtoTest {


  @Test
  public void testModificationAfterWrite() throws Exception {
    IntMessage.Builder record = IntMessage.newBuilder().setInt(776);

    Path file = tempDirectory();

    ProtoParquetWriter<MessageOrBuilder> writer =
            new ProtoParquetWriter<MessageOrBuilder>(file, IntMessage.class);

    writer.write(record);
    record.setInt(666);
    writer.close();

    IntMessageOrBuilder nextRecord = (IntMessageOrBuilder) readMessages(file).get(0);

    assertEquals(776, nextRecord.getInt());
  }

  @Test
  public void testWriteIntMessage() throws Exception {
    IntMessage record = IntMessage.newBuilder().setInt(776).build();

    IntMessageOrBuilder nextRecord = writeAndRead(record).get(0);

    assertEquals(776, nextRecord.getInt());
  }

  @Test
  public void testWriteEnumMessage() throws Exception {
    TestProtobuf.EnumMessage.Builder record = TestProtobuf.EnumMessage.newBuilder().setEnum(TestProtobuf.EnumMessage.Example.EnumValueA);

    TestProtobuf.EnumMessageOrBuilder nextRecord = writeAndRead(record).get(0);

    assertEquals(TestProtobuf.EnumMessage.Example.EnumValueA, nextRecord.getEnum());
  }


  /**
   * Tests that i can write both builder and message.
   * */
  @Test
   public void testWriteBuilder() throws Exception {
    AllInOneMessage.Builder msg1 = AllInOneMessage.newBuilder();
    AllInOneSubMessage.Builder subMsg1 = msg1.getOptionalMsgBuilder();

    AllInOneMessage.Builder msg2 = AllInOneMessage.newBuilder();
    AllInOneSubMessage.Builder subMsg2 = msg1.getOptionalMsgBuilder();

    msg1.setRequiredString(" : Pulp fiction");
    subMsg1.setOptionalString("Jan Sverak: Tmavomodry svet");

    msg2.setRequiredString(" : Snatch");
    subMsg2.setOptionalString(":Das Experiment");
    subMsg2.addRepeatedInt(2005);

    testData(msg1, msg2);
  }




  @Test
  /**
   * If i write two messages, both writes are independent.
   * */
  public void testTwoMessages() throws Exception {
    IntMessageOrBuilder record1 = IntMessage.newBuilder().setInt(776);
    IntMessageOrBuilder record2 = IntMessage.newBuilder().setInt(1223);

    List<TestProtobuf.IntMessageOrBuilder> result = writeAndRead(record1, record2);

    assertEquals(776, result.get(0).getInt());
    assertEquals(1223, result.get(1).getInt());
    assertEquals(2, result.size());
  }

  @Test
  public void testMessageArray() throws Exception {
    TestProtobuf.RepeatedInnerMessage.Builder record1 = TestProtobuf.RepeatedInnerMessage.newBuilder();

    record1.addInternalBuilder().setInt(11);
    record1.addInternalBuilder().setInt(22);


    List<TestProtobuf.RepeatedInnerMessageOrBuilder> result = writeAndRead((TestProtobuf.RepeatedInnerMessageOrBuilder) record1);
    IntMessageOrBuilder resultInternal1 = result.get(0).getInternalOrBuilder(0);
    IntMessageOrBuilder resultInternal2 = result.get(0).getInternalOrBuilder(1);

    assertEquals(11, resultInternal1.getInt());
    assertEquals(22, resultInternal2.getInt());
  }


  @Test
  public void testArray() throws Exception {
    fail("Not implemented");
  }

  @Test
  public void testInnerMessage() throws Exception {
    fail("Not implemented");
  }

  @Test
  public void testGroup() throws Exception {
    fail("Not implemented");
  }

  @Test
  public void testMessage() throws Exception {


    TestProtobuf.WebPage.Builder record1 = TestProtobuf.WebPage.newBuilder();
    record1.setUrl("http://goout.cz");
    TestProtobuf.InternalMessage.Builder internalMsg = record1.getInternalMsgBuilder();
    internalMsg.setI(7878).setStr("someString");

    List<TestProtobuf.WebPageOrBuilder> result = writeAndRead((TestProtobuf.WebPageOrBuilder) record1);
    TestProtobuf.InternalMessage resultInternal = result.get(0).getInternalMsg();

    assertEquals(7878, resultInternal.getI());
    assertEquals("someString", resultInternal.getStr());
  }

  @Test
  public void testString() throws Exception {
    TestProtobuf.StringMessageOrBuilder record1 = StringMessage.newBuilder().setName("as").setDescription("Des2");
    TestProtobuf.StringMessageOrBuilder record2 = StringMessage.newBuilder().setName("bbas").setDescription("Des1");

    List<TestProtobuf.StringMessageOrBuilder> x = writeAndRead(record1, record2);

    assertEquals("as", x.get(0).getName());
    assertEquals("Des2", x.get(0).getDescription());
    assertEquals("bbas", x.get(1).getName());
    assertEquals("Des1", x.get(1).getDescription());
  }
}
