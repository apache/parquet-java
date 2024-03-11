package org.apache.parquet.proto;

import static org.apache.parquet.proto.TestUtils.readMessages;
import static org.apache.parquet.proto.TestUtils.someTemporaryFilePath;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.proto.test.TestProto3;
import org.junit.Test;

public class ProtoParquetWriterTest {
  @Test
  public void testProtoParquetWriterWithDynamicMessage() throws Exception {
    Path file = someTemporaryFilePath();
    Descriptors.Descriptor descriptor = TestProto3.InnerMessage.getDescriptor();
    TestProto3.InnerMessage.Builder msg = TestProto3.InnerMessage.newBuilder();
    msg.setOne("oneValue");
    DynamicMessage dynamicMessage = DynamicMessage.newBuilder(msg.build()).build();

    Configuration conf = new Configuration();
    ParquetWriter<DynamicMessage> writer = ProtoParquetWriter.<DynamicMessage>builder(file)
        .withDescriptor(descriptor)
        .withConf(conf)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build();
    writer.write(dynamicMessage);
    writer.close();

    readMessages(file, TestProto3.InnerMessage.class);
    List<TestProto3.InnerMessage> gotBack = TestUtils.readMessages(file, TestProto3.InnerMessage.class);

    TestProto3.InnerMessage getFirst = gotBack.get(0);
    assertEquals(getFirst.getOne(), "oneValue");
    assertEquals(getFirst.getTwo(), "");
    assertEquals(getFirst.getThree(), "");
  }
}
