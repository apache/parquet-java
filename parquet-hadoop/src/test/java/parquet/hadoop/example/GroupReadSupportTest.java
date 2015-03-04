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
package parquet.hadoop.example;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import parquet.hadoop.api.ReadSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;

public class GroupReadSupportTest {

  private String fullSchemaStr = "message example {\n" +
          "required int32 line;\n" +
          "optional binary content;\n" +
          "}";

  private String partialSchemaStr = "message example {\n" +
          "required int32 line;\n" +
          "}";


  @Test
  public void testInitWithoutSpecifyingRequestSchema() throws Exception {
    GroupReadSupport s = new GroupReadSupport();
    Configuration configuration = new Configuration();
    Map<String, String> keyValueMetaData = new HashMap<String, String>();
    MessageType fileSchema = MessageTypeParser.parseMessageType(fullSchemaStr);

    ReadSupport.ReadContext context = s.init(configuration, keyValueMetaData, fileSchema);
    assertEquals(context.getRequestedSchema(), fileSchema);
  }

  @Test
  public void testInitWithPartialSchema() {
    GroupReadSupport s = new GroupReadSupport();
    Configuration configuration = new Configuration();
    Map<String, String> keyValueMetaData = new HashMap<String, String>();
    MessageType fileSchema = MessageTypeParser.parseMessageType(fullSchemaStr);
    MessageType partialSchema = MessageTypeParser.parseMessageType(partialSchemaStr);
    configuration.set(ReadSupport.PARQUET_READ_SCHEMA, partialSchemaStr);

    ReadSupport.ReadContext context = s.init(configuration, keyValueMetaData, fileSchema);
    assertEquals(context.getRequestedSchema(), partialSchema);
  }
}
