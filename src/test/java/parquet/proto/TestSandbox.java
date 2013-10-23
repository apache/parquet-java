package parquet.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtocolMessageEnum;
import org.junit.Test;
import parquet.protobuf.test.TestProtobuf;

public class TestSandbox {

  @Test
  public void testSandbox() throws Exception {
    TestProtobuf.EnumMessage.Builder b = TestProtobuf.EnumMessage.newBuilder();
    b.setEnum(TestProtobuf.EnumMessage.Example.EnumValueA);
  }

  @Test
  public void XXA() throws Exception {
    TestProtobuf.EnumMessage.Builder b = TestProtobuf.EnumMessage.newBuilder();
    b.setEnum(TestProtobuf.EnumMessage.Example.EnumValueA);


    ProtocolMessageEnum[] enumValues = TestProtobuf.EnumMessage.Example.values();
    for (ProtocolMessageEnum value : enumValues) {
      int index = value.getNumber();
      String name = value.getValueDescriptor().getName();
      System.out.println(name + " " + index);
    }
  }

  @Test
  public void testName() throws Exception {
    TestProtobuf.EnumMessage.Example x = TestProtobuf.EnumMessage.Example.EnumValueB;
    Descriptors.EnumValueDescriptor valueDescriptor;
    valueDescriptor = x.getValueDescriptor();

    Descriptors.FieldDescriptor fieldDescriptor = TestProtobuf.EnumMessage.getDescriptor().findFieldByName("enum");
    TestProtobuf.EnumMessage.Builder builder = TestProtobuf.EnumMessage.newBuilder();
    builder.setField(fieldDescriptor, valueDescriptor);


    System.out.println(builder.getEnum());
  }
}
