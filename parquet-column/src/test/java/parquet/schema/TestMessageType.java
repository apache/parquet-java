/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.schema;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import parquet.example.Paper;
import parquet.parser.MessageTypeParser;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;


public class TestMessageType {
  @Test
  public void test() throws Exception {
    System.out.println(Paper.schema.toString());
    MessageType schema = MessageTypeParser.parseMessageType(Paper.schema.toString());
    assertEquals(Paper.schema, schema);
    assertEquals(schema.toString(), Paper.schema.toString());
  }
  
  @Test
  public void testNestedTypes() {
    MessageType schema = MessageTypeParser.parseMessageType(Paper.schema.toString());
    Type type = schema.getType("Links", "Backward");
    assertEquals(PrimitiveTypeName.INT64,
        type.asPrimitiveType().getPrimitiveTypeName());
    assertEquals(0, schema.getMaxRepetitionLevel("DocId"));
    assertEquals(1, schema.getMaxRepetitionLevel("Name"));
    assertEquals(2, schema.getMaxRepetitionLevel("Name", "Language"));
    assertEquals(0, schema.getMaxDefinitionLevel("DocId"));
    assertEquals(1, schema.getMaxDefinitionLevel("Links"));
    assertEquals(2, schema.getMaxDefinitionLevel("Links", "Backward"));
  }
}
