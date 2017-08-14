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
package org.apache.parquet.column.mem;

import static org.junit.Assert.assertEquals;

import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.mem.MemPageWriter;
import org.junit.Test;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMemColumn {
  private static final Logger LOG = LoggerFactory.getLogger(TestMemColumn.class);

  @Test
  public void testMemColumn() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message msg { required group foo { required int64 bar; } }");
    ColumnDescriptor path = schema.getColumnDescription(new String[] {"foo", "bar"});
    MemPageStore memPageStore = new MemPageStore(10);
    ColumnWriteStoreV1 memColumnsStore = newColumnWriteStoreImpl(memPageStore);
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write(42l, 0, 0);
    memColumnsStore.flush();

    ColumnReader columnReader = getColumnReader(memPageStore, path, schema);
    for (int i = 0; i < columnReader.getTotalValueCount(); i++) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getLong(), 42);
      columnReader.consume();
    }
  }

  private ColumnWriter getColumnWriter(ColumnDescriptor path, MemPageStore memPageStore) {
    ColumnWriteStoreV1 memColumnsStore = newColumnWriteStoreImpl(memPageStore);
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    return columnWriter;
  }

  private ColumnReader getColumnReader(MemPageStore memPageStore, ColumnDescriptor path, MessageType schema) {
    return new ColumnReadStoreImpl(
        memPageStore,
        new DummyRecordConverter(schema).getRootConverter(),
        schema,
        null
        ).getColumnReader(path);
  }

  @Test
  public void testMemColumnBinary() throws Exception {
    MessageType mt = MessageTypeParser.parseMessageType("message msg { required group foo { required binary bar; } }");
    String[] col = new String[]{"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore(10);

    ColumnWriteStoreV1 memColumnsStore = newColumnWriteStoreImpl(memPageStore);
    ColumnDescriptor path1 = mt.getColumnDescription(col);
    ColumnDescriptor path = path1;

    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    columnWriter.write(Binary.fromString("42"), 0, 0);
    memColumnsStore.flush();

    ColumnReader columnReader = getColumnReader(memPageStore, path, mt);
    for (int i = 0; i < columnReader.getTotalValueCount(); i++) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getBinary().toStringUsingUTF8(), "42");
      columnReader.consume();
    }
  }

  @Test
  public void testMemColumnSeveralPages() throws Exception {
    MessageType mt = MessageTypeParser.parseMessageType("message msg { required group foo { required int64 bar; } }");
    String[] col = new String[]{"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore(10);
    ColumnWriteStoreV1 memColumnsStore = newColumnWriteStoreImpl(memPageStore);
    ColumnDescriptor path1 = mt.getColumnDescription(col);
    ColumnDescriptor path = path1;

    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    for (int i = 0; i < 2000; i++) {
      columnWriter.write(42l, 0, 0);
    }
    memColumnsStore.flush();

    ColumnReader columnReader = getColumnReader(memPageStore, path, mt);
    for (int i = 0; i < columnReader.getTotalValueCount(); i++) {
      assertEquals(columnReader.getCurrentRepetitionLevel(), 0);
      assertEquals(columnReader.getCurrentDefinitionLevel(), 0);
      assertEquals(columnReader.getLong(), 42);
      columnReader.consume();
    }
  }

  @Test
  public void testBloomColumn() throws Exception {
    MessageType schema = MessageTypeParser.parseMessageType("message test { required int32 foo; }");
    ColumnDescriptor path = schema.getColumns().get(0);
    MemPageStore memPageStore = new MemPageStore(10);
    ColumnWriteStoreV1 memColumnsStore = new ColumnWriteStoreV1(memPageStore,
      ParquetProperties.builder()
        .withPageSize(2048)
        .withDictionaryEncoding(false)
        .withBloomFilterEnabled(true)
        .withBloomFilterColumnNames("foo")
        .build());
    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    for (int i=0; i<10; i++) {
      columnWriter.write(i, 0, 0);
    }
    memColumnsStore.flush();

    ColumnReader columnReader = getColumnReader(memPageStore, path, schema);
    for(int i=0; i<columnReader.getTotalValueCount(); i++) {
      assertEquals(i, columnReader.getInteger());
      columnReader.consume();
    }
  }

  @Test
  public void testMemColumnSeveralPagesRepeated() throws Exception {
    MessageType mt = MessageTypeParser.parseMessageType("message msg { repeated group foo { repeated int64 bar; } }");
    String[] col = new String[]{"foo", "bar"};
    MemPageStore memPageStore = new MemPageStore(10);
    ColumnWriteStoreV1 memColumnsStore = newColumnWriteStoreImpl(memPageStore);
    ColumnDescriptor path1 = mt.getColumnDescription(col);
    ColumnDescriptor path = path1;

    ColumnWriter columnWriter = memColumnsStore.getColumnWriter(path);
    int[] rs = { 0, 0, 0, 1, 1, 1, 2, 2, 2};
    int[] ds = { 0, 1, 2, 0, 1, 2, 0, 1, 2};
    for (int i = 0; i < 837; i++) {
      int r = rs[i % rs.length];
      int d = ds[i % ds.length];
      LOG.debug("write i: {}", i);
      if (d == 2) {
        columnWriter.write((long)i, r, d);
      } else {
        columnWriter.writeNull(r, d);
      }
    }
    memColumnsStore.flush();

    ColumnReader columnReader = getColumnReader(memPageStore, path, mt);
    int i = 0;
    for (int j = 0; j < columnReader.getTotalValueCount(); j++) {
      int r = rs[i % rs.length];
      int d = ds[i % ds.length];
      LOG.debug("read i: {}", i);
      assertEquals("r row " + i, r, columnReader.getCurrentRepetitionLevel());
      assertEquals("d row " + i, d, columnReader.getCurrentDefinitionLevel());
      if (d == 2) {
        assertEquals("data row " + i, (long)i, columnReader.getLong());
      }
      columnReader.consume();
      ++ i;
    }
  }

  private ColumnWriteStoreV1 newColumnWriteStoreImpl(MemPageStore memPageStore) {
    return new ColumnWriteStoreV1(memPageStore,
        ParquetProperties.builder()
            .withPageSize(2048)
            .withDictionaryEncoding(false)
            .build());
  }
}
