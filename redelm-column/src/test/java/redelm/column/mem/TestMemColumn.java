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
package redelm.column.mem;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import redelm.column.ColumnDescriptor;
import redelm.column.ColumnReader;
import redelm.column.ColumnWriter;
import redelm.parser.MessageTypeParser;
import redelm.schema.MessageType;

public class TestMemColumn {
  @Test
  public void testMemColumn() throws Exception {
    System.out.println("<<<");
    MessageType mt = MessageTypeParser.parseMessageType("message msg { required group foo { required int64 bar; } }");
    MemColumnsStore memColumnsStore = new MemColumnsStore(1024, mt);
    ColumnDescriptor path = mt.getColumnDescription(new String[]{"foo", "bar"});
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write(42l, 0, 0);
    memColumnsStore.flip();
    ColumnReader columnReader = memColumnsStore.getColumnReader(path);
    System.out.println(memColumnsStore.toString());
    System.out.println("value, r, d");
    while (!columnReader.isFullyConsumed()) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getLong(), 42);
      System.out.println(columnReader.getLong()
          +", "+columnReader.getCurrentRepetitionLevel()
          +", "+columnReader.getCurrentDefinitionLevel());
      columnReader.consume();
    }
    System.out.println(">>>");
  }

  @Test
  public void testMemColumnString() throws Exception {
    System.out.println("<<<");
    MessageType mt = MessageTypeParser.parseMessageType("message msg { required group foo { required string bar; } }");
    MemColumnsStore memColumnsStore = new MemColumnsStore(1024, mt);
    ColumnDescriptor path = mt.getColumnDescription(new String[]{"foo", "bar"});
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write("42", 0, 0);
    memColumnsStore.flip();
    ColumnReader columnReader = memColumnsStore.getColumnReader(path);
    System.out.println(memColumnsStore.toString());
    System.out.println("value, r, d");
    while (!columnReader.isFullyConsumed()) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getString(), "42");
      System.out.println(columnReader.getString()
          +", "+columnReader.getCurrentRepetitionLevel()
          +", "+columnReader.getCurrentDefinitionLevel());
      columnReader.consume();
    }
    System.out.println(">>>");
  }
}
