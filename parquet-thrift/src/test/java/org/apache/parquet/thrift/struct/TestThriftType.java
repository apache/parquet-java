/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.thrift.struct;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;
import org.junit.Test;

public class TestThriftType {

  @Test
  public void testWriteUnionInfo() throws Exception {
    StructType st = new StructType(new LinkedList<ThriftField>(), null);
    assertEquals(
        ("{\n"
                + "  \"id\" : \"STRUCT\",\n"
                + "  \"children\" : [ ],\n"
                + "  \"structOrUnionType\" : \"STRUCT\",\n"
                + "  \"logicalTypeAnnotation\" : null\n"
                + "}")
            .replace("\n", System.lineSeparator()),
        st.toJSON());

    st = new StructType(new LinkedList<ThriftField>(), StructOrUnionType.UNION);
    assertEquals(
        ("{\n"
                + "  \"id\" : \"STRUCT\",\n"
                + "  \"children\" : [ ],\n"
                + "  \"structOrUnionType\" : \"UNION\",\n"
                + "  \"logicalTypeAnnotation\" : null\n"
                + "}")
            .replace("\n", System.lineSeparator()),
        st.toJSON());

    st = new StructType(new LinkedList<ThriftField>(), StructOrUnionType.STRUCT);
    assertEquals(
        ("{\n"
                + "  \"id\" : \"STRUCT\",\n"
                + "  \"children\" : [ ],\n"
                + "  \"structOrUnionType\" : \"STRUCT\",\n"
                + "  \"logicalTypeAnnotation\" : null\n"
                + "}")
            .replace("\n", System.lineSeparator()),
        st.toJSON());
  }

  @Test
  public void testParseUnionInfo() throws Exception {
    StructType st = (StructType)
        StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[], \"structOrUnionType\": \"UNION\"}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.UNION);
    st = (StructType)
        StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[], \"structOrUnionType\": \"STRUCT\"}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.STRUCT);
    st = (StructType) StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[]}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.STRUCT);
    st = (StructType)
        StructType.fromJSON("{\"id\": \"STRUCT\", \"children\":[], \"structOrUnionType\": \"UNKNOWN\"}");
    assertEquals(st.getStructOrUnionType(), StructOrUnionType.UNKNOWN);
  }
}
