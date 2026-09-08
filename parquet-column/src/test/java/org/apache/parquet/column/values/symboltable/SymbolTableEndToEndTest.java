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
package org.apache.parquet.column.values.symboltable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.page.mem.MemPageReader;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.column.page.mem.MemPageWriter;
import org.apache.parquet.column.values.symboltable.SymbolTablePayload.OffsetEncoding;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

/**
 * Values through the column machinery, not just through the values writer/reader pair: a
 * {@link MemPageStore}, a real {@link ColumnWriteStoreV1}, a real {@link ColumnReadStoreImpl}, the
 * transport built in {@code column/page} and {@code column/impl} carrying the table between them.
 *
 * <p>{@link SymbolTableValuesRoundTripTest} already covers the writer and reader in isolation; what
 * only this level can catch is a table that never reaches the writer's chunk-finalize hook, or a
 * reader built before the table it needs exists.
 */
public class SymbolTableEndToEndTest {

  private static final ColumnDescriptor REQUIRED_BINARY =
      requiredBinaryColumn().getColumnDescription(new String[] {"foo", "bar"});

  private static MessageType requiredBinaryColumn() {
    return MessageTypeParser.parseMessageType("message msg { required group foo { required binary bar; } }");
  }

  private static List<Binary> binaries(String... values) {
    List<Binary> result = new ArrayList<>();
    for (String value : values) {
      result.add(Binary.fromString(value));
    }
    return result;
  }

  /** Each word repeated enough times that FSST beats the delta-byte-array fallback it competes with. */
  private static List<Binary> repeatedBinaries(String... words) {
    List<Binary> result = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      for (String word : words) {
        result.add(Binary.fromString(word));
      }
    }
    return result;
  }

  private static ColumnWriteStoreV1 fsstWriteStore(MemPageStore memPageStore) {
    return fsstWriteStore(memPageStore, OffsetEncoding.DELTA_BINARY_PACKED);
  }

  private static ColumnWriteStoreV1 fsstWriteStore(MemPageStore memPageStore, OffsetEncoding offsetEncoding) {
    return new ColumnWriteStoreV1(
        memPageStore,
        ParquetProperties.builder()
            .withDictionaryEncoding(false)
            .withFsstEncoding(true)
            .withSymbolTableOffsetEncoding(offsetEncoding)
            .build());
  }

  private static void writeChunk(ColumnWriteStoreV1 store, ColumnDescriptor path, List<Binary> values) {
    ColumnWriter writer = store.getColumnWriter(path);
    for (Binary value : values) {
      writer.write(value, 0, 0);
      store.endRecord();
    }
    store.flush();
  }

  private static ColumnReader columnReader(MemPageStore memPageStore, ColumnDescriptor path, MessageType schema) {
    return new ColumnReadStoreImpl(memPageStore, new DummyRecordConverter(schema).getRootConverter(), schema, null)
        .getColumnReader(path);
  }

  /** Reads every value of a required column, in order. */
  private static List<Binary> readBinaries(MemPageStore memPageStore, ColumnDescriptor path, MessageType schema) {
    ColumnReader reader = columnReader(memPageStore, path, schema);
    List<Binary> read = new ArrayList<>();
    long count = reader.getTotalValueCount();
    for (long i = 0; i < count; i++) {
      read.add(reader.getBinary());
      reader.consume();
    }
    return read;
  }

  private static MemPageWriter pageWriterFor(MemPageStore memPageStore, ColumnDescriptor path) {
    return (MemPageWriter) memPageStore.getPageWriter(path);
  }

  @Test
  public void aHighCardinalityColumnKeepsTheEncodingAndPublishesATable() {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      values.add(Binary.fromString("https://example.com/catalogue/item/" + i + "?ref=newsletter"));
    }

    MemPageStore memPageStore = new MemPageStore(values.size());
    writeChunk(fsstWriteStore(memPageStore), path, values);

    MemPageWriter pageWriter = pageWriterFor(memPageStore, path);
    assertThat(pageWriter.getSymbolTablePage())
        .as("a chunk that wins with FSST publishes a table")
        .isNotNull();
    for (DataPage page : pageWriter.getPages()) {
      assertThat(((DataPageV1) page).getValueEncoding()).isEqualTo(Encoding.FSST);
    }

    assertThat(readBinaries(memPageStore, path, schema)).isEqualTo(values);
  }

  @Test
  public void multiplePagesShareOneTable() {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    String[] themes = {"warehouse-inventory", "flight-departure", "clinical-observation", "seismic-reading"};
    List<Binary> values = new ArrayList<>();
    for (String theme : themes) {
      for (int i = 0; i < 400; i++) {
        values.add(Binary.fromString(theme + "/" + i));
      }
    }

    MemPageStore memPageStore = new MemPageStore(values.size());
    ColumnWriteStoreV1 store = new ColumnWriteStoreV1(
        memPageStore,
        ParquetProperties.builder()
            .withDictionaryEncoding(false)
            .withFsstEncoding(true)
            .withPageSize(2048)
            .withMinRowCountForPageSizeCheck(1)
            .build());
    writeChunk(store, path, values);

    MemPageWriter pageWriter = pageWriterFor(memPageStore, path);
    assertThat(pageWriter.getPages().size())
        .as("the chunk crossed the page-size threshold")
        .isGreaterThan(1);
    assertThat(pageWriter.getSymbolTablePage())
        .as("one table for the whole chunk")
        .isNotNull();

    assertThat(readBinaries(memPageStore, path, schema)).isEqualTo(values);
  }

  @Test
  public void nullsAndEmptyStringsSurviveAnOptionalColumn() {
    MessageType schema = MessageTypeParser.parseMessageType("message msg { optional binary foo; }");
    ColumnDescriptor path = schema.getColumns().get(0);

    List<Binary> present = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      present.add(Binary.fromString(i % 7 == 0 ? "" : "value-" + (i % 11)));
    }

    MemPageStore memPageStore = new MemPageStore(present.size() * 2);
    ColumnWriteStoreV1 store = fsstWriteStore(memPageStore);
    ColumnWriter writer = store.getColumnWriter(path);
    List<Binary> expected = new ArrayList<>();
    for (int i = 0; i < present.size(); i++) {
      if (i % 3 == 0) {
        writer.writeNull(0, 0);
        expected.add(null);
      } else {
        Binary value = present.get(i);
        writer.write(value, 0, 1);
        expected.add(value);
      }
      store.endRecord();
    }
    store.flush();

    ColumnReader reader = columnReader(memPageStore, path, schema);
    List<Binary> read = new ArrayList<>();
    long count = reader.getTotalValueCount();
    for (long i = 0; i < count; i++) {
      read.add(reader.getCurrentDefinitionLevel() == 0 ? null : reader.getBinary());
      reader.consume();
    }
    assertThat(read).isEqualTo(expected);
  }

  @Test
  public void aColumnThatExpandsFallsBackAndPublishesNoTable() {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    List<Binary> values = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      values.add(Binary.fromConstantByteArray(new byte[] {(byte) i}));
    }

    MemPageStore memPageStore = new MemPageStore(values.size());
    writeChunk(fsstWriteStore(memPageStore, OffsetEncoding.PLAIN), path, values);

    MemPageWriter pageWriter = pageWriterFor(memPageStore, path);
    assertThat(pageWriter.getSymbolTablePage())
        .as("a chunk that fell back publishes no table")
        .isNull();
    for (DataPage page : pageWriter.getPages()) {
      assertThat(((DataPageV1) page).getValueEncoding()).isEqualTo(Encoding.PLAIN);
    }

    assertThat(readBinaries(memPageStore, path, schema)).isEqualTo(values);
  }

  @Test
  public void aColumnCanBeReadTwiceAndSkippedAtSeveralPositions() {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    List<Binary> values =
        binaries("alpha", "", "alphabet", "beta", "betamax", "gamma", "gamma-ray", "delta", "delta-force");

    MemPageStore memPageStore = new MemPageStore(values.size());
    writeChunk(fsstWriteStore(memPageStore), path, values);

    // Read the whole column once, from the top.
    assertThat(readBinaries(memPageStore, path, schema)).isEqualTo(values);

    // Read it again from the same store, skipping every other value this time.
    ColumnReader reader = columnReader(memPageStore, path, schema);
    for (int i = 0; i < values.size(); i++) {
      if (i % 2 == 0) {
        reader.skip();
      } else {
        assertThat(reader.getBinary()).as("at " + i).isEqualTo(values.get(i));
      }
      reader.consume();
    }
  }

  @Test
  public void bothOffsetEncodingsRoundTripTheSameValues() {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    List<Binary> values = binaries("one", "two", "three", "four", "five", "six", "seven");

    for (OffsetEncoding offsetEncoding : OffsetEncoding.values()) {
      MemPageStore memPageStore = new MemPageStore(values.size());
      writeChunk(fsstWriteStore(memPageStore, offsetEncoding), path, values);

      assertThat(readBinaries(memPageStore, path, schema))
          .as("offsets " + offsetEncoding)
          .isEqualTo(values);
    }
  }

  /**
   * Two independent chunks, each with its own {@link MemPageStore}, standing in for two row groups:
   * {@code MemPageWriter.writeSymbolTablePage} rejects a second table on the same block, so a
   * literal {@code resetDictionary()} mid-chunk cannot be driven through this substrate — but two
   * chunks with disjoint vocabularies prove the same thing a row-group boundary needs, and more
   * visibly: a table carried over into the wrong chunk decodes garbage, not just a bigger page.
   */
  @Test
  public void twoRowGroupsEachTrainTheirOwnTable() throws IOException {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    List<Binary> firstRowGroup = repeatedBinaries("alpha", "alphabet", "alpine", "alpaca");
    List<Binary> secondRowGroup = repeatedBinaries("zeta", "zenith", "zephyr", "zodiac");

    MemPageStore first = new MemPageStore(firstRowGroup.size());
    writeChunk(fsstWriteStore(first), path, firstRowGroup);
    MemPageStore second = new MemPageStore(secondRowGroup.size());
    writeChunk(fsstWriteStore(second), path, secondRowGroup);

    assertThat(readBinaries(first, path, schema)).isEqualTo(firstRowGroup);
    assertThat(readBinaries(second, path, schema)).isEqualTo(secondRowGroup);

    byte[] firstTable =
        pageWriterFor(first, path).getSymbolTablePage().getBytes().toByteArray();
    byte[] secondTable =
        pageWriterFor(second, path).getSymbolTablePage().getBytes().toByteArray();
    assertThat(secondTable).as("each row group trains its own table").isNotEqualTo(firstTable);
  }

  /**
   * The negative control the plan asks for: strip the symbol table out of the page reader the way a
   * corrupt or truncated read might, and confirm the column reader refuses to guess rather than
   * silently decoding as if the encoding were something else.
   *
   * <p>{@link org.apache.parquet.column.impl.ColumnReaderImpl}'s constructor calls {@code consume()}
   * to prime the first value, so the failure happens at {@code getColumnReader(path)} itself, not on
   * some later value read.
   */
  @Test
  public void aReaderCannotSilentlyProceedWhenTheTableIsMissing() {
    MessageType schema = requiredBinaryColumn();
    ColumnDescriptor path = REQUIRED_BINARY;
    List<Binary> values = repeatedBinaries("alpha", "alphabet", "alpine", "alpaca");
    MemPageStore memPageStore = new MemPageStore(values.size());
    writeChunk(fsstWriteStore(memPageStore), path, values);

    MemPageWriter pageWriter = pageWriterFor(memPageStore, path);
    assertThat(pageWriter.getSymbolTablePage())
        .as("the column actually used the encoding")
        .isNotNull();
    List<DataPage> pages = pageWriter.getPages();

    PageReadStore strippedStore = new PageReadStore() {
      @Override
      public PageReader getPageReader(ColumnDescriptor descriptor) {
        Iterator<DataPage> iterator = new ArrayList<>(pages).iterator();
        return new MemPageReader(
            pageWriter.getTotalValueCount(), iterator, pageWriter.getDictionaryPage(), null);
      }

      @Override
      public long getRowCount() {
        return memPageStore.getRowCount();
      }
    };

    assertThatThrownBy(() -> new ColumnReadStoreImpl(
                strippedStore, new DummyRecordConverter(schema).getRootConverter(), schema, null)
            .getColumnReader(path))
        .isInstanceOf(ParquetDecodingException.class);
  }
}
