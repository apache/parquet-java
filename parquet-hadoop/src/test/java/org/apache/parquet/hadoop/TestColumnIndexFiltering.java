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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.apache.parquet.filter2.predicate.LogicalInverter.invert;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optional;
import static org.apache.parquet.schema.Types.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.Location;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.PhoneNumber;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.User;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for high level column index based filtering.
 */
@RunWith(Parameterized.class)
public class TestColumnIndexFiltering {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestColumnIndexFiltering.class);
  private static final Random RANDOM = new Random(42);
  private static final String[] PHONE_KINDS = { null, "mobile", "home", "work" };
  private static final List<User> DATA = Collections.unmodifiableList(generateData(10000));
  private static final Path FILE_V1 = createTempFile();
  private static final Path FILE_V2 = createTempFile();
  private static final MessageType SCHEMA_WITHOUT_NAME = Types.buildMessage()
      .required(INT64).named("id")
      .optionalGroup()
        .addField(optional(DOUBLE).named("lon"))
        .addField(optional(DOUBLE).named("lat"))
        .named("location")
      .optionalGroup()
        .repeatedGroup()
          .addField(required(INT64).named("number"))
          .addField(optional(BINARY).as(stringType()).named("kind"))
          .named("phone")
        .named("phoneNumbers")
      .named("user_without_name");

  @Parameters
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[] { FILE_V1 }, new Object[] { FILE_V2 });
  }

  private final Path file;

  public TestColumnIndexFiltering(Path file) {
    this.file = file;
  }

  private static List<User> generateData(int rowCount) {
    List<User> users = new ArrayList<>();
    List<String> names = generateNames(rowCount);
    for (int i = 0; i < rowCount; ++i) {
      users.add(new User(i, names.get(i), generatePhoneNumbers(), generateLocation(i, rowCount)));
    }
    return users;
  }

  private static List<String> generateNames(int rowCount) {
    List<String> list = new ArrayList<>();

    // Adding fix values for filtering
    list.add("anderson");
    list.add("anderson");
    list.add("miller");
    list.add("miller");
    list.add("miller");
    list.add("thomas");
    list.add("thomas");
    list.add("williams");

    int nullCount = rowCount / 100;

    String alphabet = "aabcdeefghiijklmnoopqrstuuvwxyz";
    int maxLength = 8;
    for (int i = rowCount - list.size() - nullCount; i >= 0; --i) {
      int l = RANDOM.nextInt(maxLength);
      StringBuilder builder = new StringBuilder(l);
      for (int j = 0; j < l; ++j) {
        builder.append(alphabet.charAt(RANDOM.nextInt(alphabet.length())));
      }
      list.add(builder.toString());
    }
    Collections.sort(list, (str1, str2) -> -str1.compareTo(str2));

    // Adding nulls to random places
    for (int i = 0; i < nullCount; ++i) {
      list.add(RANDOM.nextInt(list.size()), null);
    }

    return list;
  }

  private static List<PhoneNumber> generatePhoneNumbers() {
    int length = RANDOM.nextInt(5) - 1;
    if (length < 0) {
      return null;
    }
    List<PhoneNumber> phoneNumbers = new ArrayList<>(length);
    for (int i = 0; i < length; ++i) {
      // 6 digits numbers
      long number = Math.abs(RANDOM.nextLong() % 900000) + 100000;
      phoneNumbers.add(new PhoneNumber(number, PHONE_KINDS[RANDOM.nextInt(PHONE_KINDS.length)]));
    }
    return phoneNumbers;
  }

  private static Location generateLocation(int id, int rowCount) {
    if (RANDOM.nextDouble() < 0.01) {
      return null;
    }

    double lat = RANDOM.nextDouble() * 90.0 - (id < rowCount / 2 ? 90.0 : 0.0);
    double lon = RANDOM.nextDouble() * 90.0 - (id < rowCount / 4 || id >= 3 * rowCount / 4 ? 90.0 : 0.0);

    return new Location(RANDOM.nextDouble() < 0.01 ? null : lat, RANDOM.nextDouble() < 0.01 ? null : lon);
  }

  private static Path createTempFile() {
    try {
      return new Path(Files.createTempFile("test-ci_", ".parquet").toAbsolutePath().toString());
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  private List<User> readUsers(FilterPredicate filter, boolean useOtherFiltering) throws IOException {
    return readUsers(FilterCompat.get(filter), useOtherFiltering, true);
  }

  private List<User> readUsers(FilterPredicate filter, boolean useOtherFiltering, boolean useColumnIndexFilter)
      throws IOException {
    return readUsers(FilterCompat.get(filter), useOtherFiltering, useColumnIndexFilter);
  }

  private List<User> readUsers(Filter filter, boolean useOtherFiltering) throws IOException {
    return readUsers(filter, useOtherFiltering, true);
  }

  private List<User> readUsers(Filter filter, boolean useOtherFiltering, boolean useColumnIndexFilter)
      throws IOException {
    return PhoneBookWriter.readUsers(ParquetReader.builder(new GroupReadSupport(), file)
        .withFilter(filter)
        .useDictionaryFilter(useOtherFiltering)
        .useStatsFilter(useOtherFiltering)
        .useRecordFilter(useOtherFiltering)
        .useColumnIndexFilter(useColumnIndexFilter));
  }

  private List<User> readUsersWithProjection(Filter filter, MessageType schema, boolean useOtherFiltering, boolean useColumnIndexFilter) throws IOException {
    return PhoneBookWriter.readUsers(ParquetReader.builder(new GroupReadSupport(), file)
        .withFilter(filter)
        .useDictionaryFilter(useOtherFiltering)
        .useStatsFilter(useOtherFiltering)
        .useRecordFilter(useOtherFiltering)
        .useColumnIndexFilter(useColumnIndexFilter)
        .set(ReadSupport.PARQUET_READ_SCHEMA, schema.toString()));
  }

  // Assumes that both lists are in the same order
  private static void assertContains(Stream<User> expected, List<User> actual) {
    Iterator<User> expIt = expected.iterator();
    if (!expIt.hasNext()) {
      return;
    }
    User exp = expIt.next();
    for (User act : actual) {
      if (act.equals(exp)) {
        if (!expIt.hasNext()) {
          break;
        }
        exp = expIt.next();
      }
    }
    assertFalse("Not all expected elements are in the actual list. E.g.: " + exp, expIt.hasNext());
  }

  private void assertCorrectFiltering(Predicate<User> expectedFilter, FilterPredicate actualFilter)
      throws IOException {
    // Check with only column index based filtering
    List<User> result = readUsers(actualFilter, false);

    assertTrue("Column-index filtering should drop some pages", result.size() < DATA.size());
    LOGGER.info("{}/{} records read; filtering ratio: {}%", result.size(), DATA.size(),
        100 * result.size() / DATA.size());
    // Asserts that all the required records are in the result
    assertContains(DATA.stream().filter(expectedFilter), result);
    // Asserts that all the retrieved records are in the file (validating non-matching records)
    assertContains(result.stream(), DATA);

    // Check with all the filtering filtering to ensure the result contains exactly the required values
    result = readUsers(actualFilter, true);
    assertEquals(DATA.stream().filter(expectedFilter).collect(Collectors.toList()), result);
  }

  @BeforeClass
  public static void createFile() throws IOException {
    int pageSize = DATA.size() / 10;     // Ensure that several pages will be created
    int rowGroupSize = pageSize * 6 * 5; // Ensure that there are more row-groups created
    PhoneBookWriter.write(ExampleParquetWriter.builder(FILE_V1)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withWriterVersion(WriterVersion.PARQUET_1_0),
        DATA);
    PhoneBookWriter.write(ExampleParquetWriter.builder(FILE_V2)
        .withWriteMode(OVERWRITE)
        .withRowGroupSize(rowGroupSize)
        .withPageSize(pageSize)
        .withWriterVersion(WriterVersion.PARQUET_2_0),
        DATA);
  }

  @AfterClass
  public static void deleteFile() throws IOException {
    FILE_V1.getFileSystem(new Configuration()).delete(FILE_V1, false);
    FILE_V2.getFileSystem(new Configuration()).delete(FILE_V2, false);
  }

  @Test
  public void testSimpleFiltering() throws IOException {
    assertCorrectFiltering(
        record -> record.getId() == 1234,
        eq(longColumn("id"), 1234l));
    assertCorrectFiltering(
        record -> "miller".equals(record.getName()),
        eq(binaryColumn("name"), Binary.fromString("miller")));
    assertCorrectFiltering(
        record -> record.getName() == null,
        eq(binaryColumn("name"), null));
  }

  @Test
  public void testNoFiltering() throws IOException {
    // Column index filtering with no-op filter
    assertEquals(DATA, readUsers(FilterCompat.NOOP, false));
    assertEquals(DATA, readUsers(FilterCompat.NOOP, true));

    // Column index filtering turned off
    assertEquals(DATA.stream().filter(user -> user.getId() == 1234).collect(Collectors.toList()),
        readUsers(eq(longColumn("id"), 1234l), true, false));
    assertEquals(DATA.stream().filter(user -> "miller".equals(user.getName())).collect(Collectors.toList()),
        readUsers(eq(binaryColumn("name"), Binary.fromString("miller")), true, false));
    assertEquals(DATA.stream().filter(user -> user.getName() == null).collect(Collectors.toList()),
        readUsers(eq(binaryColumn("name"), null), true, false));

    // Every filtering mechanism turned off
    assertEquals(DATA, readUsers(eq(longColumn("id"), 1234l), false, false));
    assertEquals(DATA, readUsers(eq(binaryColumn("name"), Binary.fromString("miller")), false, false));
    assertEquals(DATA, readUsers(eq(binaryColumn("name"), null), false, false));
  }

  @Test
  public void testComplexFiltering() throws IOException {
    assertCorrectFiltering(
        record -> {
          Location loc = record.getLocation();
          Double lat = loc == null ? null : loc.getLat();
          Double lon = loc == null ? null : loc.getLon();
          return lat != null && lon != null && 37 <= lat && lat <= 70 && -21 <= lon && lon <= 35;
        },
        and(and(gtEq(doubleColumn("location.lat"), 37.0), ltEq(doubleColumn("location.lat"), 70.0)),
            and(gtEq(doubleColumn("location.lon"), -21.0), ltEq(doubleColumn("location.lon"), 35.0))));
    assertCorrectFiltering(
        record -> {
          Location loc = record.getLocation();
          return loc == null || (loc.getLat() == null && loc.getLon() == null);
        },
        and(eq(doubleColumn("location.lat"), null), eq(doubleColumn("location.lon"), null)));
    assertCorrectFiltering(
        record -> {
          String name = record.getName();
          return name != null && name.compareTo("thomas") < 0 && record.getId() <= 3 * DATA.size() / 4;
        },
        and(lt(binaryColumn("name"), Binary.fromString("thomas")), ltEq(longColumn("id"), 3l * DATA.size() / 4)));
  }

  public static class NameStartsWithVowel extends UserDefinedPredicate<Binary> {
    private static final Binary A = Binary.fromString("a");
    private static final Binary B = Binary.fromString("b");
    private static final Binary E = Binary.fromString("e");
    private static final Binary F = Binary.fromString("f");
    private static final Binary I = Binary.fromString("i");
    private static final Binary J = Binary.fromString("j");
    private static final Binary O = Binary.fromString("o");
    private static final Binary P = Binary.fromString("p");
    private static final Binary U = Binary.fromString("u");
    private static final Binary V = Binary.fromString("v");

    private static boolean isStartingWithVowel(String str) {
      if (str == null || str.isEmpty()) {
        return false;
      }
      switch (str.charAt(0)) {
        case 'a':
        case 'e':
        case 'i':
        case 'o':
        case 'u':
          return true;
        default:
          return false;
      }
    }

    @Override
    public boolean keep(Binary value) {
      return value != null && isStartingWithVowel(value.toStringUsingUTF8());
    }

    @Override
    public boolean canDrop(Statistics<Binary> statistics) {
      Comparator<Binary> cmp = statistics.getComparator();
      Binary min = statistics.getMin();
      Binary max = statistics.getMax();
      return cmp.compare(max, A) < 0
          || (cmp.compare(min, B) >= 0 && cmp.compare(max, E) < 0)
          || (cmp.compare(min, F) >= 0 && cmp.compare(max, I) < 0)
          || (cmp.compare(min, J) >= 0 && cmp.compare(max, O) < 0)
          || (cmp.compare(min, P) >= 0 && cmp.compare(max, U) < 0)
          || cmp.compare(min, V) >= 0;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Binary> statistics) {
      Comparator<Binary> cmp = statistics.getComparator();
      Binary min = statistics.getMin();
      Binary max = statistics.getMax();
      return (cmp.compare(min, A) >= 0 && cmp.compare(max, B) < 0)
          || (cmp.compare(min, E) >= 0 && cmp.compare(max, F) < 0)
          || (cmp.compare(min, I) >= 0 && cmp.compare(max, J) < 0)
          || (cmp.compare(min, O) >= 0 && cmp.compare(max, P) < 0)
          || (cmp.compare(min, U) >= 0 && cmp.compare(max, V) < 0);
    }
  }

  public static class IsDivisibleBy extends UserDefinedPredicate<Long> implements Serializable {
    private long divisor;

    IsDivisibleBy(long divisor) {
      this.divisor = divisor;
    }

    @Override
    public boolean keep(Long value) {
      // Deliberately not checking for null to verify the handling of NPE
      // Implementors shall always checks the value for null and return accordingly
      return value % divisor == 0;
    }

    @Override
    public boolean canDrop(Statistics<Long> statistics) {
      long min = statistics.getMin();
      long max = statistics.getMax();
      return min % divisor != 0 && max % divisor != 0 && min / divisor == max / divisor;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Long> statistics) {
      long min = statistics.getMin();
      long max = statistics.getMax();
      return min == max && min % divisor == 0;
    }
  }

  @Test
  public void testUDF() throws IOException {
    assertCorrectFiltering(
        record -> NameStartsWithVowel.isStartingWithVowel(record.getName()) || record.getId() % 234 == 0,
        or(userDefined(binaryColumn("name"), NameStartsWithVowel.class),
            userDefined(longColumn("id"), new IsDivisibleBy(234))));
    assertCorrectFiltering(
        record -> !(NameStartsWithVowel.isStartingWithVowel(record.getName()) || record.getId() % 234 == 0),
            not(or(userDefined(binaryColumn("name"), NameStartsWithVowel.class),
                userDefined(longColumn("id"), new IsDivisibleBy(234)))));
  }

  @Test
  public void testFilteringWithMissingColumns() throws IOException {
    // Missing column filter is always true
    assertEquals(DATA, readUsers(notEq(binaryColumn("not-existing-binary"), Binary.EMPTY), true));
    assertCorrectFiltering(
        record -> record.getId() == 1234,
        and(eq(longColumn("id"), 1234l),
            eq(longColumn("not-existing-long"), null)));
    assertCorrectFiltering(
        record -> "miller".equals(record.getName()),
        and(eq(binaryColumn("name"), Binary.fromString("miller")),
            invert(userDefined(binaryColumn("not-existing-binary"), NameStartsWithVowel.class))));

    // Missing column filter is always false
    assertEquals(emptyList(), readUsers(lt(longColumn("not-existing-long"), 0l), true));
    assertCorrectFiltering(
        record -> "miller".equals(record.getName()),
        or(eq(binaryColumn("name"), Binary.fromString("miller")),
            gtEq(binaryColumn("not-existing-binary"), Binary.EMPTY)));
    assertCorrectFiltering(
        record -> record.getId() == 1234,
        or(eq(longColumn("id"), 1234l),
            userDefined(longColumn("not-existing-long"), new IsDivisibleBy(1))));
  }

  @Test
  public void testFilteringWithProjection() throws IOException {
    // All rows shall be retrieved because all values in column 'name' shall be handled as null values
    assertEquals(
        DATA.stream().map(user -> user.cloneWithName(null)).collect(toList()),
        readUsersWithProjection(FilterCompat.get(eq(binaryColumn("name"), null)), SCHEMA_WITHOUT_NAME, true, true));

    // Column index filter shall drop all pages because all values in column 'name' shall be handled as null values
    assertEquals(
        emptyList(),
        readUsersWithProjection(FilterCompat.get(notEq(binaryColumn("name"), null)), SCHEMA_WITHOUT_NAME, false, true));
    assertEquals(
        emptyList(),
        readUsersWithProjection(FilterCompat.get(userDefined(binaryColumn("name"), NameStartsWithVowel.class)),
            SCHEMA_WITHOUT_NAME, false, true));
  }
}
