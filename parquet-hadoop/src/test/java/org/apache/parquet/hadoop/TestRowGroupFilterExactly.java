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

import static org.apache.parquet.filter2.compat.PredicateEvaluation.BLOCK_CANNOT_MATCH;
import static org.apache.parquet.filter2.compat.PredicateEvaluation.BLOCK_MUST_MATCH;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.BLOOMFILTER;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.DICTIONARY;
import static org.apache.parquet.filter2.compat.RowGroupFilter.FilterLevel.STATISTICS;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notIn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.PredicateEvaluation;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class TestRowGroupFilterExactly {
  private final Path FILE = createTempFile();
  private ParquetProperties.WriterVersion WRITER_VERSION;
  private final Random RANDOM = new Random(42);
  private String MIN_NAME = "";
  private String MAX_NAME = "";
  private final long MIN_ID = 0;
  private final List<PhoneBookWriter.User> DATA = Collections.unmodifiableList(generateData(10000));
  private final long MAX_ID = DATA.size() - 1;
  private String NON_EXIST_NAME = "meller";
  private final TestPredicateEvaluation testEvaluation = new TestPredicateEvaluation();

  @Parameterized.Parameters(name = "Run parquet version {index} ")
  public static Collection<Object[]> params() {
    return Arrays.asList(
      new Object[]{ParquetProperties.WriterVersion.PARQUET_1_0},
      new Object[]{ParquetProperties.WriterVersion.PARQUET_2_0});
  }

  public TestRowGroupFilterExactly(ParquetProperties.WriterVersion WRITER_VERSION) throws IOException {
    this.WRITER_VERSION = WRITER_VERSION;
    deleteFile(FILE);
    writePhoneBookToFile(FILE, this.WRITER_VERSION);
  }

  @After
  public void deleteFiles() throws IOException {
    deleteFile(FILE);
    testEvaluation.setTestExactPredicate(new ArrayList<>(Arrays.asList(BLOCK_MUST_MATCH, BLOCK_CANNOT_MATCH)));
  }

  @Test
  public void testFiltering() throws IOException {

    Set<Binary> existValues = new HashSet<>();
    existValues.add(Binary.fromString("miller"));
    existValues.add(Binary.fromString("anderson"));

    //     StatisticsFilter: left must match , right must match -> must match
    assertCorrectFiltering(or(gtEq(binaryColumn("name"), Binary.fromString(MIN_NAME)),
      gtEq(longColumn("id"), MIN_ID)));
    //     StatisticsFilter: left must match, right might match -> must match
    assertCorrectFiltering(or(gtEq(binaryColumn("name"), Binary.fromString(MIN_NAME)),
      eq(longColumn("id"), 1234L)));
    //     StatisticsFilter: left can't match, right must match -> must match
    assertCorrectFiltering(or(gt(binaryColumn("name"), Binary.fromString(MAX_NAME)),
      gtEq(longColumn("id"), MIN_ID)));
    //     StatisticsFilter: left can't match, right can't match -> can't match
    assertCorrectFiltering(or(gt(binaryColumn("name"), Binary.fromString(MAX_NAME)),
      gt(longColumn("id"), MAX_ID)));
    // StatisticsFilter: left might match (can't match in DictionaryFilter), right must match -> must match
    assertCorrectFiltering(or(eq(binaryColumn("name"), Binary.fromString(NON_EXIST_NAME)),
      gtEq(longColumn("id"), MIN_ID)));

    // StatisticsFilter: left must match , right must match -> must match
    assertCorrectFiltering(and(gtEq(binaryColumn("name"), Binary.fromString(MIN_NAME)),
      gtEq(longColumn("id"), MIN_ID)));
    // StatisticsFilter: left must match, right might match -> might match
    assertCorrectFiltering(and(gtEq(binaryColumn("name"), Binary.fromString(MIN_NAME)),
      eq(longColumn("id"), 1234L)));
    // StatisticsFilter: left can't match, right must match -> can't match
    assertCorrectFiltering(and(gt(binaryColumn("name"), Binary.fromString(MAX_NAME)),
      gtEq(longColumn("id"), MIN_ID)));
    // In StatisticsFilter left might match (but can't match in DictionaryFilter), right must match
    // -> return might match in StatisticsFilter, return can't match in DictionaryFilter
    assertCorrectFiltering(and(eq(binaryColumn("name"), Binary.fromString(NON_EXIST_NAME)),
      gtEq(longColumn("id"), MIN_ID)));
    //     StatisticsFilter: left can't match, right can't match -> can't match
    assertCorrectFiltering(or(gt(binaryColumn("name"), Binary.fromString(MAX_NAME)),
      gt(longColumn("id"), MAX_ID)));

    assertCorrectFiltering(eq(binaryColumn("name"), null));
    assertCorrectFiltering(eq(binaryColumn("name"), Binary.fromString("miller")));
    assertCorrectFiltering(eq(longColumn("id"), 1234L));
    assertCorrectFiltering(eq(binaryColumn("name"), Binary.fromString(NON_EXIST_NAME)));
    assertCorrectFiltering(eq(doubleColumn("location.lat"), 99.9));

    assertCorrectFiltering(notEq(binaryColumn("name"), null));
    assertCorrectFiltering(notEq(binaryColumn("name"), Binary.fromString("miller")));
    assertCorrectFiltering(notEq(binaryColumn("name"), Binary.fromString(NON_EXIST_NAME)));

    assertCorrectFiltering(in(binaryColumn("name"), existValues));
    assertCorrectFiltering(in(binaryColumn("name"), Sets.newHashSet(Binary.fromString("miller"),
      Binary.fromString(NON_EXIST_NAME), null)));

    assertCorrectFiltering(notIn(binaryColumn("name"),
      Sets.newHashSet(Binary.fromString("miller"), Binary.fromString("anderson"))));
    assertCorrectFiltering(notIn(binaryColumn("name"),
      Sets.newHashSet(Binary.fromString("miller"), Binary.fromString(NON_EXIST_NAME), null)));

    assertCorrectFiltering(lt(longColumn("id"), MAX_ID + 1L));
    assertCorrectFiltering(lt(longColumn("id"), MAX_ID));
    assertCorrectFiltering(lt(longColumn("id"), 1234L));
    assertCorrectFiltering(lt(longColumn("id"), MIN_ID));
    assertCorrectFiltering(lt(longColumn("id"), MIN_ID - 1L));
    // for dictionary exactly match less than `miller`
    assertCorrectFiltering(lt(binaryColumn("name"), Binary.fromString(NON_EXIST_NAME)));
    assertCorrectFiltering(lt(binaryColumn("name"), Binary.fromString("miller")));

    assertCorrectFiltering(ltEq(longColumn("id"), MAX_ID + 1L));
    assertCorrectFiltering(ltEq(longColumn("id"), MAX_ID));
    assertCorrectFiltering(ltEq(longColumn("id"), 1234L));
    assertCorrectFiltering(ltEq(longColumn("id"), MIN_ID));
    assertCorrectFiltering(ltEq(longColumn("id"), MIN_ID - 1L));

    assertCorrectFiltering(gt(longColumn("id"), MAX_ID + 1L));
    assertCorrectFiltering(gt(longColumn("id"), MAX_ID));
    assertCorrectFiltering(gt(longColumn("id"), 1234L));
    assertCorrectFiltering(gt(longColumn("id"), MIN_ID));
    assertCorrectFiltering(gt(longColumn("id"), MIN_ID - 1L));

    assertCorrectFiltering(gtEq(longColumn("id"), MAX_ID + 1L));
    assertCorrectFiltering(gtEq(longColumn("id"), MAX_ID));
    assertCorrectFiltering(gtEq(longColumn("id"), 1234L));
    assertCorrectFiltering(gtEq(longColumn("id"), MIN_ID));
    assertCorrectFiltering(gtEq(longColumn("id"), MIN_ID - 1L));

    ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(FILE, new Configuration()));
    for (int i = 0; i < 1000; i++) {
      PhoneBookWriter.User user = DATA.get(i);
      if (user.getName() == null) {
        continue;
      }
      assertCorrectFiltering(eq(binaryColumn("name"), Binary.fromString(user.getName())), reader);
      assertCorrectFiltering(notEq(binaryColumn("name"), Binary.fromString(user.getName())), reader);
      assertCorrectFiltering(lt(binaryColumn("name"), Binary.fromString(user.getName())), reader);
      assertCorrectFiltering(ltEq(binaryColumn("name"), Binary.fromString(user.getName())), reader);
      assertCorrectFiltering(gt(binaryColumn("name"), Binary.fromString(user.getName())), reader);
      assertCorrectFiltering(gtEq(binaryColumn("name"), Binary.fromString(user.getName())), reader);
      assertCorrectFiltering(in(binaryColumn("name"), Sets.newHashSet(Binary.fromString(user.getName()))), reader);
      assertCorrectFiltering(notIn(binaryColumn("name"), Sets.newHashSet(Binary.fromString(user.getName()))), reader);

      assertCorrectFiltering(eq(longColumn("id"), user.getId()), reader);
      assertCorrectFiltering(notEq(longColumn("id"), user.getId()), reader);
      assertCorrectFiltering(lt(longColumn("id"), user.getId()), reader);
      assertCorrectFiltering(ltEq(longColumn("id"), user.getId()), reader);
      assertCorrectFiltering(gt(longColumn("id"), user.getId()), reader);
      assertCorrectFiltering(gtEq(longColumn("id"), user.getId()), reader);
      assertCorrectFiltering(in(longColumn("id"), Sets.newHashSet(user.getId())), reader);
      assertCorrectFiltering(notIn(longColumn("id"), Sets.newHashSet(user.getId())), reader);
    }
  }

  private void assertCorrectFiltering(FilterPredicate filter) throws IOException {
    ParquetReadOptions readOptions = ParquetReadOptions.builder()
      .withRecordFilter(FilterCompat.get(filter)).build();

    // simulate the previous behavior, only skip other filters when predicate is BLOCK_CANNOT_MATCH
    testEvaluation.setTestExactPredicate(Collections.singletonList(BLOCK_CANNOT_MATCH));
    List<BlockMetaData> rowGroups2 =
      ParquetFileReader.open(HadoopInputFile.fromPath(FILE, new Configuration()), readOptions).getRowGroups();

    // when predicate is BLOCK_CANNOT_MATCH or BLOCK_MUST_MATCH, the other filters will be skipped for optimization
    testEvaluation.setTestExactPredicate(new ArrayList<>(Arrays.asList(BLOCK_MUST_MATCH, BLOCK_CANNOT_MATCH)));
    List<BlockMetaData> rowGroups1 =
      ParquetFileReader.open(HadoopInputFile.fromPath(FILE, new Configuration()), readOptions).getRowGroups();

    // the filtered rowGroups should be same
    assertTrue(isEqualRowGroups(rowGroups1, rowGroups2));
  }

  private void assertCorrectFiltering(FilterPredicate filter, ParquetFileReader reader) {
    List<BlockMetaData> originBlocks = reader.getRowGroups();
    List<RowGroupFilter.FilterLevel> levels = Lists.newArrayList(STATISTICS, DICTIONARY, BLOOMFILTER);
    // simulate the previous behavior, only skip other filters when predicate is BLOCK_CANNOT_MATCH
    testEvaluation.setTestExactPredicate(Collections.singletonList(BLOCK_CANNOT_MATCH));
    List<BlockMetaData> rowGroups1 = RowGroupFilter.filterRowGroups(levels, FilterCompat.get(filter), originBlocks, reader);

    // when predicate is BLOCK_CANNOT_MATCH or BLOCK_MUST_MATCH, the other filters will be skipped for optimization
    testEvaluation.setTestExactPredicate(new ArrayList<>(Arrays.asList(BLOCK_MUST_MATCH, BLOCK_CANNOT_MATCH)));
    List<BlockMetaData> rowGroups2 = RowGroupFilter.filterRowGroups(levels, FilterCompat.get(filter), originBlocks, reader);

    // the filtered rowGroups should be same
    assertTrue(isEqualRowGroups(rowGroups1, rowGroups2));
  }

  private boolean isEqualRowGroups(List<BlockMetaData> left, List<BlockMetaData> right) {
    if (left.size() != right.size()) {
      return false;
    }
    Set<Long> offsets1 = left.stream().map(BlockMetaData::getRowIndexOffset).collect(Collectors.toSet());
    Set<Long> offsets2 = right.stream().map(BlockMetaData::getRowIndexOffset).collect(Collectors.toSet());
    return offsets1.containsAll(offsets2);
  }

  private void writePhoneBookToFile(Path file,
    ParquetProperties.WriterVersion parquetVersion) throws IOException {
    int pageSize = DATA.size() / 100;     // Ensure that several pages will be created
    int rowGroupSize = pageSize * 4;    // Ensure that there are more row-groups created
    PhoneBookWriter.write(ExampleParquetWriter.builder(file)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withBloomFilterNDV("location.lat", 10000L)
        .withBloomFilterNDV("name", 10000L)
        .withBloomFilterNDV("id", 10000L)
        .withWriterVersion(parquetVersion),
      DATA);
  }


  private Path createTempFile() {
    try {
      return new Path(Files.createTempFile("test-exact-predicate-filter", ".parquet").toAbsolutePath().toString());
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private void deleteFile(Path file) throws IOException {
    file.getFileSystem(new Configuration()).delete(file, false);
  }

  private List<PhoneBookWriter.User> generateData(int rowCount) {
    List<PhoneBookWriter.User> users = new ArrayList<>();
    List<String> names = generateNames(rowCount);
    for (int i = 0; i < rowCount; ++i) {
      users.add(new PhoneBookWriter.User(i, names.get(i), generatePhoneNumbers(), generateLocation(i, rowCount)));
    }
    return users;
  }

  private List<String> generateNames(int rowCount) {
    List<String> list = new ArrayList<>();

    // Adding fix values for filtering
    for (int i = 0; i < rowCount / 1000; i++) {
      list.add("miller");
    }
    list.add("anderson");
    list.add("thomas");
    list.add("thomas");
    list.add("williams");
    int nullCount = 5;
    // avoid adding this name
    String alphabet = "aabcdeefghiijklmnoopqrstuuvwxyz";
    int maxLength = 8;
    for (int i = rowCount - list.size() - nullCount; i >= 0; ) {
      int l = RANDOM.nextInt(maxLength);
      StringBuilder builder = new StringBuilder(l);
      for (int j = 0; j < l; ++j) {
        builder.append(alphabet.charAt(RANDOM.nextInt(alphabet.length())));
      }
      if (builder.toString().equals(NON_EXIST_NAME)) {
        continue;
      } else {
        list.add(builder.toString());
        i--;
      }
    }
    list.sort((str1, str2) -> -str1.compareTo(str2));
    MAX_NAME = list.get(0);
    MIN_NAME = list.get(list.size() - 1);
    // Adding nulls to random places
    for (int i = 0; i < nullCount; ++i) {
      list.add(RANDOM.nextInt(list.size()), null);
    }
    return list;
  }

  private List<PhoneBookWriter.PhoneNumber> generatePhoneNumbers() {
    int length = RANDOM.nextInt(5) - 1;
    if (length < 0) {
      return null;
    }
    List<PhoneBookWriter.PhoneNumber> phoneNumbers = new ArrayList<>(length);
    String[] PHONE_KINDS = {null, "mobile", "home", "work"};
    for (int i = 0; i < length; ++i) {
      // 6 digits numbers
      long number = Math.abs(RANDOM.nextLong() % 900000) + 100000;
      phoneNumbers.add(new PhoneBookWriter.PhoneNumber(number, PHONE_KINDS[RANDOM.nextInt(PHONE_KINDS.length)]));
    }
    return phoneNumbers;
  }

  private PhoneBookWriter.Location generateLocation(int id, int rowCount) {
    if (RANDOM.nextDouble() < 0.01) {
      return null;
    }
    if (RANDOM.nextDouble() < 0.001) {
      return new PhoneBookWriter.Location(99.9, 99.9);
    }
    double lat = RANDOM.nextDouble() * 90.0 - (id < rowCount / 2 ? 90.0 : 0.0);
    double lon = RANDOM.nextDouble() * 90.0 - (id < rowCount / 4 || id >= 3 * rowCount / 4 ? 90.0 : 0.0);
    return new PhoneBookWriter.Location(RANDOM.nextDouble() < 0.01 ? null : lat, RANDOM.nextDouble() < 0.01 ? null : lon);
  }

  private class TestPredicateEvaluation extends PredicateEvaluation {
    // Only for Unit Test
    private void setTestExactPredicate(List<Boolean> predicate) {
      EXACT_PREDICATES = predicate;
    }
  }
}
