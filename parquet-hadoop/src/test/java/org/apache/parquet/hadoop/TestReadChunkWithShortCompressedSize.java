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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Some old writers recorded a total_compressed_size that did not include the size of the dictionary
 * page header, so the column chunk extends past its recorded size. Verifies that the last column
 * chunk of a row group is still read completely in that case.
 */
public class TestReadChunkWithShortCompressedSize {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message test { required binary name (STRING); }");
  private static final int ROWS = 2_000;
  // the dictionary page header written by those writers was usually 13 to 19 bytes
  private static final int MISSING_DICTIONARY_HEADER_BYTES = 13;

  @TempDir
  private Path tempDir;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReadWhenLastPageIsTruncated(boolean vectoredIo) throws IOException {
    Path file = writeFile();
    ColumnChunkMetaData column = readColumnChunk(file);

    Path shortened = copyWithTotalCompressedSize(file, column.getTotalSize() - MISSING_DICTIONARY_HEADER_BYTES);

    assertThat(readNames(shortened, vectoredIo)).containsExactlyElementsOf(expectedNames());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testReadWhenLastPageHeaderIsTruncated(boolean vectoredIo) throws IOException {
    Path file = writeFile();
    ColumnChunkMetaData column = readColumnChunk(file);
    OffsetIndex offsetIndex = readOffsetIndex(file, column);
    long lastPageOffset = offsetIndex.getOffset(offsetIndex.getPageCount() - 1);

    // end the chunk two bytes into the header of its last page
    Path shortened = copyWithTotalCompressedSize(file, lastPageOffset + 2 - column.getStartingPos());

    assertThat(readNames(shortened, vectoredIo)).containsExactlyElementsOf(expectedNames());
  }

  private Path writeFile() throws IOException {
    Path file = tempDir.resolve("original.parquet");
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
        .withType(SCHEMA)
        .withDictionaryEncoding(true)
        .withPageRowCountLimit(500)
        .build()) {
      for (String name : expectedNames()) {
        writer.write(factory.newGroup().append("name", name));
      }
    }
    return file;
  }

  private static List<String> expectedNames() {
    List<String> names = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++) {
      names.add("name-" + (i % 50));
    }
    return names;
  }

  private static ColumnChunkMetaData readColumnChunk(Path file) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      assertThat(reader.getRowGroups()).hasSize(1);
      ColumnChunkMetaData column =
          reader.getRowGroups().get(0).getColumns().get(0);
      assertThat(column.hasDictionaryPage()).isTrue();
      return column;
    }
  }

  private static OffsetIndex readOffsetIndex(Path file, ColumnChunkMetaData column) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
      OffsetIndex offsetIndex = reader.readOffsetIndex(column);
      assertThat(offsetIndex.getPageCount()).isGreaterThan(1);
      return offsetIndex;
    }
  }

  /**
   * Copies the file, replacing its footer with one that records the given total_compressed_size for
   * its only column chunk, like the old writers did.
   */
  private Path copyWithTotalCompressedSize(Path src, long totalCompressedSize) throws IOException {
    byte[] bytes = Files.readAllBytes(src);
    int footerLen = ByteBuffer.wrap(bytes, bytes.length - 8, 4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .getInt();
    int footerStart = bytes.length - 8 - footerLen;
    FileMetaData footer = Util.readFileMetaData(new ByteArrayInputStream(bytes, footerStart, footerLen));
    ColumnMetaData columnMetaData =
        footer.getRow_groups().get(0).getColumns().get(0).getMeta_data();
    assertThat(totalCompressedSize).isLessThan(columnMetaData.getTotal_compressed_size());
    columnMetaData.setTotal_compressed_size(totalCompressedSize);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write(bytes, 0, footerStart);
    ByteArrayOutputStream serialized = new ByteArrayOutputStream();
    Util.writeFileMetaData(footer, serialized);
    serialized.writeTo(out);
    out.write(ByteBuffer.allocate(4)
        .order(ByteOrder.LITTLE_ENDIAN)
        .putInt(serialized.size())
        .array());
    out.write(ParquetFileWriter.MAGIC);
    return Files.write(tempDir.resolve("shortened.parquet"), out.toByteArray());
  }

  private static List<String> readNames(Path file, boolean vectoredIo) throws IOException {
    InputFile inputFile = new LocalInputFile(file);
    ParquetReadOptions options =
        ParquetReadOptions.builder().withUseHadoopVectoredIo(vectoredIo).build();
    List<String> names = new ArrayList<>(ROWS);
    PositionalVectoredReadStream stream = new PositionalVectoredReadStream(inputFile);
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile, options, stream)) {
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
        RecordReader<Group> records = new ColumnIOFactory()
            .getColumnIO(SCHEMA)
            .getRecordReader(pages, new GroupRecordConverter(SCHEMA));
        for (long row = 0; row < pages.getRowCount(); row++) {
          names.add(records.read().getString("name", 0));
        }
      }
    }
    assertThat(stream.vectoredReads.get()).isEqualTo(vectoredIo ? 1 : 0);
    return names;
  }

  /**
   * Serves vectored reads with positioned reads that, like Hadoop's, do not move the stream.
   */
  private static final class PositionalVectoredReadStream extends DelegatingSeekableInputStream {
    private final InputFile file;
    private final SeekableInputStream delegate;
    private final AtomicInteger vectoredReads = new AtomicInteger();

    private PositionalVectoredReadStream(InputFile file) throws IOException {
      this(file, file.newStream());
    }

    private PositionalVectoredReadStream(InputFile file, SeekableInputStream delegate) {
      super(delegate);
      this.file = file;
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocator) {
      return true;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocator) throws IOException {
      vectoredReads.incrementAndGet();
      try (SeekableInputStream in = file.newStream()) {
        for (ParquetFileRange range : ranges) {
          ByteBuffer buffer = allocator.allocate(range.getLength());
          in.seek(range.getOffset());
          in.readFully(buffer);
          buffer.flip();
          range.setDataReadFuture(CompletableFuture.completedFuture(buffer));
        }
      }
    }
  }
}
