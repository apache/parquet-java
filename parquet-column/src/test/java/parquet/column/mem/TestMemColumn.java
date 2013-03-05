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
package parquet.column.mem;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import parquet.Log;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.ColumnWriter;
import parquet.column.mem.MemColumnReadStore;
import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.io.Binary;
import parquet.parser.MessageTypeParser;
import parquet.schema.MessageType;


public class TestMemColumn {
  private static final Log LOG = Log.getLog(TestMemColumn.class);

  @Test
  public void testMemColumn() throws Exception {
    String schema = "message msg { required group foo { required int64 bar; } }";
    String[] col = {"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore memColumnsStore = new MemColumnWriteStore(memPageStore, 2048);
    ColumnDescriptor path = getCol(schema, col);
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write(42l, 0, 0);
    columnWriter.flush();

    ColumnReader columnReader = new MemColumnReadStore(memPageStore).getColumnReader(path);
    while (!columnReader.isFullyConsumed()) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getLong(), 42);
      columnReader.consume();
    }
  }

  private ColumnDescriptor getCol(String schema, String[] col) {
    MessageType mt = MessageTypeParser.parseMessageType(schema);
    ColumnDescriptor path = mt.getColumnDescription(col);
    return path;
  }

  @Test
  public void testMemColumnBinary() throws Exception {
    String schema = "message msg { required group foo { required binary bar; } }";
    String[] col = new String[]{"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore memColumnsStore = new MemColumnWriteStore(memPageStore, 2048);
    ColumnDescriptor path = getCol(schema, col);

    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write(Binary.fromString("42"), 0, 0);
    columnWriter.flush();

    ColumnReader columnReader = new MemColumnReadStore(memPageStore).getColumnReader(path);
    while (!columnReader.isFullyConsumed()) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getBinary().toStringUsingUTF8(), "42");
      columnReader.consume();
    }
  }

  @Test
  public void testMemColumnSeveralPages() throws Exception {
    String schema = "message msg { required group foo { required int64 bar; } }";
    String[] col = new String[]{"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore memColumnsStore = new MemColumnWriteStore(memPageStore, 2048);
    ColumnDescriptor path = getCol(schema, col);

    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    for (int i = 0; i < 2000; i++) {
      columnWriter.write(42l, 0, 0);
    }
    columnWriter.flush();

    ColumnReader columnReader = new MemColumnReadStore(memPageStore).getColumnReader(path);
    while (!columnReader.isFullyConsumed()) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getLong(), 42);
      columnReader.consume();
    }
  }

  @Test
  public void testMemColumnSeveralPagesRepeated() throws Exception {
    String schema = "message msg { repeated group foo { repeated int64 bar; } }";
    String[] col = new String[]{"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore();
    MemColumnWriteStore memColumnsStore = new MemColumnWriteStore(memPageStore, 2048);
    ColumnDescriptor path = getCol(schema, col);

    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    int[] rs = { 0, 0, 0, 1, 1, 1, 2, 2, 2};
    int[] ds = { 0, 1, 2, 0, 1, 2, 0, 1, 2};
    for (int i = 0; i < 837; i++) {
      int r = rs[i % rs.length];
      int d = ds[i % ds.length];
      LOG.debug("write i: " + i);
      if (d == 2) {
        columnWriter.write((long)i, r, d);
      } else {
        columnWriter.writeNull(r, d);
      }
    }
    columnWriter.flush();

    ColumnReader columnReader = new MemColumnReadStore(memPageStore).getColumnReader(path);
    int i = 0;
    while (!columnReader.isFullyConsumed()) {
      int r = rs[i % rs.length];
      int d = ds[i % ds.length];
      LOG.debug("read i: " + i);
      assertEquals("r row " + i, r, columnReader.getCurrentRepetitionLevel());
      assertEquals("d row " + i, d, columnReader.getCurrentDefinitionLevel());
      if (d == 2) {
        assertEquals("data row " + i, (long)i, columnReader.getLong());
      }
      columnReader.consume();
      ++ i;
    }
  }
}
