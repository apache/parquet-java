package parquet.thrift.struct;

import java.util.LinkedList;

import org.junit.Test;

import parquet.thrift.struct.ThriftType.StructType;
import parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;

import static org.junit.Assert.assertEquals;

public class TestThriftType {

  @Test
  public void testWriteUnionInfo() throws Exception {
    StructType st = new StructType(new LinkedList<ThriftField>(), null);
    assertEquals("{\n"
                +"  \"id\" : \"STRUCT\",\n"
                +"  \"children\" : [ ],\n"
                +"  \"structOrUnionType\" : \"UNKNOWN\"\n"
                +"}", st.toJSON());

    st = new StructType(new LinkedList<ThriftField>(), StructOrUnionType.UNION);
    assertEquals("{\n"
        +"  \"id\" : \"STRUCT\",\n"
        +"  \"children\" : [ ],\n"
        +"  \"structOrUnionType\" : \"UNION\"\n"
        +"}", st.toJSON());

    st = new StructType(new LinkedList<ThriftField>(), StructOrUnionType.STRUCT);
    assertEquals("{\n"
        +"  \"id\" : \"STRUCT\",\n"
        +"  \"children\" : [ ],\n"
        +"  \"structOrUnionType\" : \"STRUCT\"\n"
        +"}", st.toJSON());
  }

  @Test
  public void testParseUnionInfo() throws Exception {
    StructType st = (StructType) StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[], \"structOrUnionType\": \"UNION\"}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.UNION);
    st = (StructType) StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[], \"structOrUnionType\": \"STRUCT\"}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.STRUCT);
    st = (StructType) StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[]}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.UNKNOWN);
    st = (StructType) StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[], \"structOrUnionType\": \"UNKNOWN\"}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.UNKNOWN);
  }
}
