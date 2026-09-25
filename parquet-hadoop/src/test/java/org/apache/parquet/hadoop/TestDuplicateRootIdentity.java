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
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.PrimitiveIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDuplicateRootIdentity {
  private static final Configuration CONF = new Configuration();

  static {
    CONF.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
    CONF.setBoolean("fs.file.impl.disable.cache", true);
  }

  private static FileMetaData footer(File file) throws Exception {
    try (RandomAccessFile input = new RandomAccessFile(file, "r")) {
      input.seek(input.length() - 8);
      int length = Integer.reverseBytes(input.readInt());
      input.seek(input.length() - 8 - length);
      byte[] bytes = new byte[length];
      input.readFully(bytes);
      return Util.readFileMetaData(new ByteArrayInputStream(bytes));
    }
  }

  private static void duplicate(File file) throws Exception {
    // Write valid pages first, then rename only the physical footer identity.
    FileMetaData metadata = footer(file);
    metadata.getSchema().stream().filter(s -> s.getName().equals("c")).forEach(s -> s.setName("a"));
    metadata.getRow_groups().forEach(g -> g.getColumns().forEach(c -> {
      if (c.getMeta_data().getPath_in_schema().equals(Arrays.asList("c"))) {
        c.getMeta_data().setPath_in_schema(Arrays.asList("a"));
      }
    }));
    try (RandomAccessFile out = new RandomAccessFile(file, "rw")) {
      out.seek(out.length() - 8);
      int oldLength = Integer.reverseBytes(out.readInt());
      long start = out.length() - 8 - oldLength;
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      Util.writeFileMetaData(metadata, bytes);
      out.seek(start);
      out.write(bytes.toByteArray());
      out.writeInt(Integer.reverseBytes(bytes.size()));
      out.writeBytes("PAR1");
      out.setLength(out.getFilePointer());
    }
  }

  private static File fixture(File dir) throws Exception {
    File file = new File(dir, "indexed.parquet");
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test { optional int64 a; " + "optional binary c; optional int64 b; }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
        .withConf(CONF)
        .withType(schema)
        .withDictionaryEncoding(false)
        .withPageSize(256)
        .withMinRowCountForPageSizeCheck(1)
        .withMaxRowCountForPageSizeCheck(1)
        .withRowGroupSize(32768)
        .build()) {
      for (int i = 0; i < 240; i++) {
        Group row = factory.newGroup().append("a", (long) i).append("b", (long) i);
        row.append("c", new String(new char[80 + i % 31]).replace('\0', 'z'));
        writer.write(row);
      }
    }
    duplicate(file);
    return file;
  }

  private static void index(File file) throws Exception {
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), CONF))) {
      BlockMetaData block = reader.getFooter().getBlocks().get(0);
      OffsetIndex first = reader.readOffsetIndex(block.getColumns().get(0));
      OffsetIndex later = reader.readOffsetIndex(block.getColumns().get(1));
      assertThat(first.getPageCount() != later.getPageCount())
          .as("fixture needs distinct page layouts")
          .isTrue();
      ColumnIndexStore store = ColumnIndexStoreImpl.create(
          reader, block, new HashSet<>(Arrays.asList(ColumnPath.get("a"), ColumnPath.get("b"))));
      OffsetIndex selected = store.getOffsetIndex(ColumnPath.get("a"));
      assertThat(selected.getOffset(0))
          .as("index must select first root chunk")
          .isEqualTo(first.getOffset(0));
      assertThat(selected.getPageCount())
          .as("index must retain first page layout")
          .isEqualTo(first.getPageCount());
      // The unselected duplicate must not even load an offset index.
      block.getColumns().get(1).setOffsetIndexReference(null);
      ColumnIndexStore withoutLater =
          ColumnIndexStoreImpl.create(reader, block, Collections.singleton(ColumnPath.get("a")));
      assertThat(withoutLater.getOffsetIndex(ColumnPath.get("a")).getOffset(0))
          .as("missing later index must not discard first index")
          .isEqualTo(first.getOffset(0));
    }
  }

  private static void partialProjection(File file) throws Exception {
    FilterPredicate filter = FilterApi.and(
        FilterApi.gtEq(FilterApi.longColumn("b"), 11L), FilterApi.lt(FilterApi.longColumn("b"), 19L));
    ParquetReadOptions options = HadoopReadOptions.builder(CONF, new Path(file.toURI()))
        .withRecordFilter(FilterCompat.get(filter))
        .useColumnIndexFilter(true)
        .build();
    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toURI()), CONF), options)) {
      MessageType projection =
          MessageTypeParser.parseMessageType("message test { optional int64 a; optional int64 b; }");
      reader.setRequestedSchema(projection);
      try (PageReadStore pages = reader.readNextFilteredRowGroup()) {
        assertThat(pages != null)
            .as("partial range must retain matching rows")
            .isTrue();
        assertThat(pages.getRowCount() < reader.getRowGroups().get(0).getRowCount())
            .as("b index must select a partial row group")
            .isTrue();
        assertThat(pages.getRowIndexes().isPresent())
            .as("partial range must expose physical row indexes")
            .isTrue();
        PrimitiveIterator.OfLong rows = pages.getRowIndexes().get();
        assertThat(rows.hasNext())
            .as("partial range must contain physical row indexes")
            .isTrue();
        ColumnReaderImpl decoder = new ColumnReaderImpl(
            projection.getColumns().get(0),
            pages.getPageReader(projection.getColumns().get(0)),
            new PrimitiveConverter() {},
            VersionParser.parse(reader.getFileMetaData().getCreatedBy()));
        while (rows.hasNext()) {
          long row = rows.nextLong();
          assertThat(decoder.getLong())
              .as("partial a page must match physical row " + row)
              .isEqualTo(row);
          decoder.consume();
        }
      }
    }
  }

  private static FileMetaData nestedFooter(File directory) throws Exception {
    File file = new File(directory, "nested.parquet");
    MessageType schema = MessageTypeParser.parseMessageType(
        "message test { optional group s { optional int32 x; optional int64 y; } " + "optional int64 a; }");
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
        .withConf(CONF)
        .withType(schema)
        .withDictionaryEncoding(false)
        .withRowGroupSize(1024)
        .withMinRowCountForPageSizeCheck(1)
        .withMaxRowCountForPageSizeCheck(1)
        .build()) {
      for (int i = 0; i < 400; i++) {
        Group row = factory.newGroup().append("a", (long) i);
        row.addGroup("s").append("x", i).append("y", (long) i + 1000);
        writer.write(row);
      }
    }
    return footer(file);
  }

  @Test
  public void physicalOrdinalIntegrity() throws Exception {
    FileMetaData raw = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    ParquetMetadata converted = new ParquetMetadataConverter().fromParquetMetadata(raw);
    assertThat(converted.getBlocks().size() > 1)
        .as("ordinal control needs multiple row groups")
        .isTrue();
    for (BlockMetaData block : converted.getBlocks()) {
      assertThat(block.getColumns().get(0).getType()).isEqualTo(PrimitiveType.PrimitiveTypeName.INT32);
      assertThat(block.getColumns().get(1).getType()).isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
      assertThat(block.getColumns().get(0).getPath()).isEqualTo(ColumnPath.get("s", "x"));
      assertThat(block.getColumns().get(1).getPath()).isEqualTo(ColumnPath.get("s", "y"));
      assertThat(block.getColumns().get(2).getPath()).isEqualTo(ColumnPath.get("a"));
      assertThat(block.getColumns().get(2).getType()).isEqualTo(PrimitiveType.PrimitiveTypeName.INT64);
    }
  }

  @Test
  public void rejectWrongColumnCount() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups().get(0).getColumns().remove(2);
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectZeroColumnChunks() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups().get(0).getColumns().clear();
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectExcessColumnChunks() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups()
        .get(0)
        .addToColumns(
            metadata.getRow_groups().get(0).getColumns().get(0).deepCopy());
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectNullOrdinalPath() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups().get(0).getColumns().get(0).getMeta_data().unsetPath_in_schema();
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectEmptyOrdinalPath() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups().get(0).getColumns().get(0).getMeta_data().setPath_in_schema(Collections.emptyList());
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void emptySchemaAndExplicitEmptyColumns() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.setSchema(
        Collections.singletonList(new org.apache.parquet.format.SchemaElement("empty").setNum_children(0)));
    metadata.unsetColumn_orders();
    metadata.getRow_groups().forEach(group -> group.setColumns(Collections.emptyList()));
    ParquetMetadata converted = new ParquetMetadataConverter().fromParquetMetadata(metadata);
    assertThat(converted.getBlocks().size()).isEqualTo(metadata.getRow_groupsSize());
    assertThat(converted.getBlocks().get(0).getColumns().isEmpty()).isTrue();
    assertThat(converted.getBlocks().get(0).getRowCount())
        .isEqualTo(metadata.getRow_groups().get(0).getNum_rows());
    metadata.getRow_groups().get(0).unsetColumns();
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void validateBeforeCryptoRegistration() throws Exception {
    for (boolean wrongType : new boolean[] {false, true}) {
      FileMetaData metadata = nestedFooter(java.nio.file.Files.createTempDirectory(temporary, "fixture")
          .toFile());
      org.apache.parquet.format.ColumnMetaData column =
          metadata.getRow_groups().get(0).getColumns().get(0).getMeta_data();
      if (wrongType) {
        column.setType(org.apache.parquet.format.Type.INT64);
      } else {
        column.setPath_in_schema(Collections.singletonList("wrong"));
      }
      org.apache.parquet.crypto.InternalFileDecryptor decryptor =
          new org.apache.parquet.crypto.InternalFileDecryptor(
              org.apache.parquet.crypto.FileDecryptionProperties.builder()
                  .withFooterKey(new byte[16])
                  .build());
      // An uninitialized decryptor rejects registration; ordinal validation must win.
      assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata, decryptor, false))
          .isInstanceOf(ParquetDecodingException.class);
    }
  }

  @Test
  public void unprojectedColumnKeyMetadataDoesNotRetrieveKey() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups().forEach(group -> group.getColumns().forEach(column -> {
      org.apache.parquet.format.EncryptionWithColumnKey key =
          new org.apache.parquet.format.EncryptionWithColumnKey(
              column.getMeta_data().getPath_in_schema());
      key.setKey_metadata(new byte[] {1, 2, 3});
      column.setCrypto_metadata(org.apache.parquet.format.ColumnCryptoMetaData.ENCRYPTION_WITH_COLUMN_KEY(key));
      column.unsetMeta_data();
      column.setEncrypted_column_metadata(new byte[] {4, 5, 6});
    }));
    java.util.concurrent.atomic.AtomicInteger retrievals = new java.util.concurrent.atomic.AtomicInteger();
    org.apache.parquet.crypto.InternalFileDecryptor decryptor = new org.apache.parquet.crypto.InternalFileDecryptor(
        org.apache.parquet.crypto.FileDecryptionProperties.builder()
            .withFooterKey(new byte[16])
            .withKeyRetriever(key -> {
              retrievals.incrementAndGet();
              throw new AssertionError("Unprojected column key retrieved");
            })
            .build());
    ParquetMetadata converted = new ParquetMetadataConverter().fromParquetMetadata(metadata, decryptor, false);
    assertThat(converted.getBlocks().size()).isEqualTo(metadata.getRow_groupsSize());
    assertThat(converted.getBlocks().get(0).getColumns().size()).isEqualTo(3);
    assertThat(retrievals.get()).isEqualTo(0);
  }

  @Test
  public void rejectWrongOrdinalPath() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups()
        .get(0)
        .getColumns()
        .get(0)
        .getMeta_data()
        .setPath_in_schema(Collections.singletonList("a"));
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @Test
  public void rejectWrongOrdinalType() throws Exception {
    FileMetaData metadata = nestedFooter(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile());
    metadata.getRow_groups()
        .get(0)
        .getColumns()
        .get(0)
        .getMeta_data()
        .setType(org.apache.parquet.format.Type.INT64);
    assertThatThrownBy(() -> new ParquetMetadataConverter().fromParquetMetadata(metadata))
        .isInstanceOf(ParquetDecodingException.class);
  }

  @TempDir
  private java.nio.file.Path temporary;

  @Test
  public void firstRootIndexes() throws Exception {
    index(fixture(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile()));
  }

  @Test
  public void firstRootPartialProjection() throws Exception {
    partialProjection(fixture(
        java.nio.file.Files.createTempDirectory(temporary, "fixture").toFile()));
  }
}
