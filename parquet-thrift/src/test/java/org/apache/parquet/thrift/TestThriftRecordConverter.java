package org.apache.parquet.thrift;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.parquet.Files;
import org.apache.parquet.Strings;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.thrift.ThriftRecordConverter.FieldEnumConverter;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftField.Requirement;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.EnumValue;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.test.compat.StructWithUnionV1;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestThriftRecordConverter {
  @Test
  public void testUnknownEnumThrowsGoodException() throws Exception {
    EnumType et = new EnumType(Arrays.asList(new EnumValue(77, "hello")));
    ThriftField field = new ThriftField("name", (short) 1, Requirement.REQUIRED, et);

    ArrayList<TProtocol> events = new ArrayList<TProtocol>();

    FieldEnumConverter conv = new  FieldEnumConverter(events, field);

    conv.addBinary(Binary.fromString("hello"));

    assertEquals(1, events.size());
    assertEquals(77, events.get(0).readI32());

    try {
      conv.addBinary(Binary.fromString("FAKE_ENUM_VALUE"));
      fail("this should throw");
    } catch (ParquetDecodingException e) {
      assertEquals("Unrecognized enum value: FAKE_ENUM_VALUE known values: {Binary{\"hello\"}=77} in {\n" +
          "  \"name\" : \"name\",\n" +
          "  \"fieldId\" : 1,\n" +
          "  \"requirement\" : \"REQUIRED\",\n" +
          "  \"type\" : {\n" +
          "    \"id\" : \"ENUM\",\n" +
          "    \"values\" : [ {\n" +
          "      \"id\" : 77,\n" +
          "      \"name\" : \"hello\"\n" +
          "    } ]\n" +
          "  }\n" +
          "}", e.getMessage());
    }
  }

  @Test
  public void constructorDoesNotRequireStructOrUnionTypeMeta() throws Exception {
    String jsonWithNoStructOrUnionMeta = Strings.join(
        Files.readAllLines(
            new File("parquet-thrift/src/test/resources/org/apache/parquet/thrift/StructWithUnionV1NoStructOrUnionMeta.json"),
            Charset.forName("UTF-8")), "\n");

    StructType noStructOrUnionMeta  = (StructType) ThriftType.fromJSON(jsonWithNoStructOrUnionMeta);

    // this used to throw, see PARQUET-346
    new ThriftRecordConverter<StructWithUnionV1>(
        new ThriftReader<StructWithUnionV1>() {
          @Override
          public StructWithUnionV1 readOneRecord(TProtocol protocol) throws TException {
            return null;
          }
        },
        "name",
        new ThriftSchemaConverter().convert(StructWithUnionV1.class),
        noStructOrUnionMeta
    );
  }
}
