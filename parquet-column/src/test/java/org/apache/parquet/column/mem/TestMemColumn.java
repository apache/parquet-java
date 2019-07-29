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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.impl.ColumnWriteStoreV2;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Test;
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
    memColumnsStore.endRecord();
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
    memColumnsStore.endRecord();
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
      memColumnsStore.endRecord();
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
      if (i != 0 && r == 0) {
        memColumnsStore.endRecord();
      }
      if (d == 2) {
        columnWriter.write((long)i, r, d);
      } else {
        columnWriter.writeNull(r, d);
      }
    }
    memColumnsStore.endRecord();
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

  @Test
  public void testPageSize() {
    MessageType schema = Types.buildMessage()
        .requiredList().requiredElement(BINARY).named("binary_col")
        .requiredList().requiredElement(INT32).named("int32_col")
        .named("msg");
    System.out.println(schema);
    MemPageStore memPageStore = new MemPageStore(123);

    // Using V2 pages so we have rowCount info
    ColumnWriteStore writeStore = new ColumnWriteStoreV2(schema, memPageStore, ParquetProperties.builder()
        .withPageSize(1024) // Less than 10 records for binary_col
        .withMinRowCountForPageSizeCheck(1) // Enforce having precise page sizing
        .withPageRowCountLimit(10)
        .withDictionaryEncoding(false) // Enforce having large binary_col pages
        .build());
    ColumnDescriptor binaryCol = schema.getColumnDescription(new String[] { "binary_col", "list", "element" });
    ColumnWriter binaryColWriter = writeStore.getColumnWriter(binaryCol);
    ColumnDescriptor int32Col = schema.getColumnDescription(new String[] { "int32_col", "list", "element" });
    ColumnWriter int32ColWriter = writeStore.getColumnWriter(int32Col);
    // Writing 123 records
    for (int i = 0; i < 123; ++i) {
      // Writing 10 values per record
      for (int j = 0; j < 10; ++j) {
        binaryColWriter.write(Binary.fromString("aaaaaaaaaaaa"), j == 0 ? 0 : 2, 2);
        int32ColWriter.write(42, j == 0 ? 0 : 2, 2);
      }
      writeStore.endRecord();
    }
    writeStore.flush();

    // Check that all the binary_col pages are <= 1024 bytes
    {
      PageReader binaryColPageReader = memPageStore.getPageReader(binaryCol);
      assertEquals(1230, binaryColPageReader.getTotalValueCount());
      int pageCnt = 0;
      int valueCnt = 0;
      while (valueCnt < binaryColPageReader.getTotalValueCount()) {
        DataPage page = binaryColPageReader.readPage();
        ++pageCnt;
        valueCnt += page.getValueCount();
        LOG.info("binary_col page-{}: {} bytes, {} rows", pageCnt, page.getCompressedSize(), page.getIndexRowCount().get());
        assertTrue("Compressed size should be less than 1024", page.getCompressedSize() <= 1024);
      }
    }

    // Check that all the int32_col pages contain <= 10 rows
    {
      PageReader int32ColPageReader = memPageStore.getPageReader(int32Col);
      assertEquals(1230, int32ColPageReader.getTotalValueCount());
      int pageCnt = 0;
      int valueCnt = 0;
      while (valueCnt < int32ColPageReader.getTotalValueCount()) {
        DataPage page = int32ColPageReader.readPage();
        ++pageCnt;
        valueCnt += page.getValueCount();
        LOG.info("int32_col page-{}: {} bytes, {} rows", pageCnt, page.getCompressedSize(), page.getIndexRowCount().get());
        assertTrue("Row count should be less than 10", page.getIndexRowCount().get() <= 10);
      }
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
