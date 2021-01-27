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
package org.apache.parquet.thrift;

import java.util.ArrayList;

import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.parquet.thrift.struct.ThriftType.StructType.StructOrUnionType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestThriftMetaData {

  /**
   * Previously, ThriftMetaData.toString would try to instantiate thriftClassName,
   * but there is no guarantee that that class is on the classpath, and it is in fact
   * normal for that to be the case (for example, when a file was written with TBase objects
   * but is being read with scrooge objects).
   *
   * See PARQUET-345
   */
  @Test
  public void testToStringDoesNotThrow() {

    StructType descriptor = new StructType(new ArrayList<ThriftField>(), StructOrUnionType.STRUCT);
    ThriftMetaData tmd = new ThriftMetaData("non existent class!!!", descriptor);
    assertEquals("ThriftMetaData(thriftClassName: non existent class!!!, descriptor: {\n" +
        "  \"id\" : \"STRUCT\",\n" +
        "  \"children\" : [ ],\n" +
        "  \"structOrUnionType\" : \"STRUCT\",\n" +
        "  \"logicalTypeAnnotation\" : null\n" +
        "})", tmd.toString());

    tmd = new ThriftMetaData("non existent class!!!", null);
    assertEquals("ThriftMetaData(thriftClassName: non existent class!!!, descriptor: null)", tmd.toString());

  }
}
