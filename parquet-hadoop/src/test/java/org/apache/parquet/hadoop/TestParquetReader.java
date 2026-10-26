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

import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestParquetReader {

  private static final Path FILE_V1 = createTempFile();
  private static final Path FILE_V2 = createTempFile();
  private static final Path STATIC_FILE_WITHOUT_COL_INDEXES =
      createPathFromCP("/test-file-with-no-column-indexes-1.parquet");
  private static final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(makeUsers(1000));

  private TrackingByteBufferAllocator allocator;

  static Stream<Arguments> data() {
    return Stream.of(Arguments.of(FILE_V1), Arguments.of(FILE_V2), Arguments.of(STATIC_FILE_WITHOUT_COL_INDEXES));
  }

  private static long fileSize(Path file) throws IOException {
    return file.getFileSystem(new Configuration()).getFileStatus(file).getLen();
  }

  private static Path createPathFromCP(String path) {
    try {
      return new Path(TestParquetReader.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeAll
  public static void createFiles() throws IOException {
    writePhoneBookToFile(FILE_V1, ParquetProperties.WriterVersion.PARQUET_1_0);
    writePhoneBookToFile(FILE_V2, ParquetProperties.WriterVersion.PARQUET_2_0);
  }

  @AfterAll
  public static void deleteFiles() throws IOException {
    deleteFile(FILE_V1);
    deleteFile(FILE_V2);
  }

  private static void deleteFile(Path file) throws IOException {
    file.getFileSystem(new Configuration()).delete(file, false);
  }

  public static List<PhoneBookWriter.User> makeUsers(int rowCount) {
    List<PhoneBookWriter.User> users = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      PhoneBookWriter.Location location = null;
      if (i % 3 == 1) {
        location = new PhoneBookWriter.Location((double) i, (double) i * 2);
      }
      if (i % 3 == 2) {
        location = new PhoneBookWriter.Location((double) i, null);
      }
      // row index of each row in the file is same as the user id.
      users.add(new PhoneBookWriter.User(
          i, "p" + i, List.of(new PhoneBookWriter.PhoneNumber(i, "cell")), location));
    }
    return users;
  }

  private static Path createTempFile() {
    try {
      return new Path(Files.createTempFile("test-ci_", ".parquet")
          .toAbsolutePath()
          .toString());
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private static void writePhoneBookToFile(Path file, ParquetProperties.WriterVersion parquetVersion)
      throws IOException {
    int pageSize = DATA.size() / 10; // Ensure that several pages will be created
    int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created

    PhoneBookWriter.write(
        ExampleParquetWriter.builder(file)
            .withWriteMode(OVERWRITE)
            .withRowGroupSize(rowGroupSize)
            .withPageSize(pageSize)
            .withWriterVersion(parquetVersion),
        DATA);
  }

  private List<PhoneBookWriter.User> readUsers(
      Path file, FilterCompat.Filter filter, boolean useOtherFiltering, boolean useColumnIndexFilter)
      throws IOException {
    return readUsers(file, filter, useOtherFiltering, useColumnIndexFilter, 0, fileSize(file));
  }

  private List<PhoneBookWriter.User> readUsers(
      Path file,
      FilterCompat.Filter filter,
      boolean useOtherFiltering,
      boolean useColumnIndexFilter,
      long rangeStart,
      long rangeEnd)
      throws IOException {
    return PhoneBookWriter.readUsers(
        ParquetReader.builder(new GroupReadSupport(), file)
            .withAllocator(allocator)
            .withFilter(filter)
            .useDictionaryFilter(useOtherFiltering)
            .useStatsFilter(useOtherFiltering)
            .useRecordFilter(useOtherFiltering)
            .useColumnIndexFilter(useColumnIndexFilter)
            .withFileRange(rangeStart, rangeEnd),
        true);
  }

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCurrentRowIndex(Path file) throws Exception {
    ParquetReader<Group> reader = PhoneBookWriter.createReader(file, FilterCompat.NOOP, allocator);
    // Fetch row index without processing any row.
    assertThat(reader.getCurrentRowIndex()).isEqualTo(-1);
    reader.read();
    assertThat(reader.getCurrentRowIndex()).isEqualTo(0);
    // calling the same API again and again should return same result.
    assertThat(reader.getCurrentRowIndex()).isEqualTo(0);

    reader.read();
    assertThat(reader.getCurrentRowIndex()).isEqualTo(1);
    assertThat(reader.getCurrentRowIndex()).isEqualTo(1);
    long expectedCurrentRowIndex = 2L;
    while (reader.read() != null) {
      assertThat(reader.getCurrentRowIndex()).isEqualTo(expectedCurrentRowIndex);
      expectedCurrentRowIndex++;
    }
    // reader.read() returned null and so reader doesn't have any more rows.
    assertThat(reader.getCurrentRowIndex()).isEqualTo(-1);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testCurrentRowGroupIndex(Path file) throws Exception {
    int expectedRowGroups;
    try (ParquetFileReader fileReader =
        ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      expectedRowGroups = fileReader.getRowGroups().size();
    }
    assertThat(expectedRowGroups)
        .as("expected multiple row groups for this test")
        .isGreaterThan(1);

    try (ParquetReader<Group> reader = PhoneBookWriter.createReader(file, FilterCompat.NOOP, allocator)) {
      // before reading anything, returns -1
      assertThat(reader.getCurrentRowGroupIndex()).isEqualTo(-1);

      reader.read();
      assertThat(reader.getCurrentRowGroupIndex()).isEqualTo(0);
      // idempotent
      assertThat(reader.getCurrentRowGroupIndex()).isEqualTo(0);

      int prevIdx = 0;
      while (reader.read() != null) {
        int idx = reader.getCurrentRowGroupIndex();
        assertThat(idx).isGreaterThanOrEqualTo(prevIdx);
        assertThat(idx).isLessThanOrEqualTo(prevIdx + 1);
        prevIdx = idx;
      }
      // last row group seen should be the final one
      assertThat(prevIdx).isEqualTo(expectedRowGroups - 1);
      // after exhaustion, returns -1
      assertThat(reader.getCurrentRowGroupIndex()).isEqualTo(-1);
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testRangeFiltering(Path file) throws Exception {
    long size = fileSize(file);
    // The readUsers also validates the rowIndex for each returned row.
    readUsers(file, FilterCompat.NOOP, false, false, size / 2, size);
    readUsers(file, FilterCompat.NOOP, true, false, size / 3, size * 3 / 4);
    readUsers(file, FilterCompat.NOOP, false, true, size / 4, size / 2);
    readUsers(file, FilterCompat.NOOP, true, true, size * 3 / 4, size);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testSimpleFiltering(Path file) throws Exception {
    Set<Long> idSet = new HashSet<>();
    idSet.add(123l);
    idSet.add(567l);
    // The readUsers also validates the rowIndex for each returned row.
    List<PhoneBookWriter.User> filteredUsers1 =
        readUsers(file, FilterCompat.get(in(longColumn("id"), idSet)), true, true);
    assertThat(filteredUsers1).hasSize(2);
    List<PhoneBookWriter.User> filteredUsers2 =
        readUsers(file, FilterCompat.get(in(longColumn("id"), idSet)), true, false);
    assertThat(filteredUsers2).hasSize(2);
    List<PhoneBookWriter.User> filteredUsers3 =
        readUsers(file, FilterCompat.get(in(longColumn("id"), idSet)), false, false);
    assertThat(filteredUsers3).hasSize(1000);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testNoFiltering(Path file) throws Exception {
    assertThat(readUsers(file, FilterCompat.NOOP, false, false)).isEqualTo(DATA);
    assertThat(readUsers(file, FilterCompat.NOOP, true, false)).isEqualTo(DATA);
    assertThat(readUsers(file, FilterCompat.NOOP, false, true)).isEqualTo(DATA);
    assertThat(readUsers(file, FilterCompat.NOOP, true, true)).isEqualTo(DATA);
  }

  private static class TestParquetReaderBuilder extends ParquetReader.Builder<Group> {

    @Override
    protected ReadSupport<Group> getReadSupport() {
      return new GroupReadSupport();
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testParquetReaderBuilderWithInputFile(Path file) throws Exception {
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    Builder<Group> builder = new TestParquetReaderBuilder().withFile(inputFile);
    assertThat(PhoneBookWriter.readUsers(builder, false)).isEqualTo(DATA);
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testParquetReaderBuilderValidatesThatInputFileCanNotBeNull(Path file) throws Exception {
    assertThatThrownBy(() -> new TestParquetReaderBuilder().withFile(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("file cannot be null");
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testParquetReaderBuilderValidatesThatInputFileIsSet(Path file) throws Exception {
    assertThatThrownBy(() -> new TestParquetReaderBuilder().build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("File or Path must be set");
  }

  @ParameterizedTest
  @MethodSource("data")
  public void testParquetReaderBuilderCanNotConfigurePathAndFile(Path file) throws Exception {
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    assertThatThrownBy(() -> ParquetReader.<Group>builder(new GroupReadSupport(), file)
            .withFile(inputFile)
            .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Path is already set");
  }
}
