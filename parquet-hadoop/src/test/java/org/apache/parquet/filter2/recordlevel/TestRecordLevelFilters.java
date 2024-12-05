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
package org.apache.parquet.filter2.recordlevel;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.contains;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.in;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.notIn;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.filter2.predicate.FilterApi.size;
import static org.apache.parquet.filter2.predicate.FilterApi.userDefined;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.DoubleColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.filter2.predicate.Statistics;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.Location;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.PhoneNumber;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter.User;
import org.apache.parquet.io.api.Binary;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRecordLevelFilters {

  public static List<User> makeUsers() {
    List<User> users = new ArrayList<User>();

    users.add(new User(
        17,
        null,
        null,
        null,
        ImmutableMap.of(
            "business", 1000.0D,
            "personal", 500.0D)));

    users.add(new User(18, "bob", null, null));

    users.add(new User(
        19,
        "alice",
        new ArrayList<PhoneNumber>(),
        null,
        ImmutableMap.of(
            "business", 2000.0D,
            "retirement", 1000.0D)));

    users.add(new User(20, "thing1", Arrays.asList(new PhoneNumber(5555555555L, null)), null));

    users.add(new User(
        27,
        "thing2",
        Arrays.asList(new PhoneNumber(1111111111L, "home"), new PhoneNumber(2222222222L, "cell")),
        null));

    users.add(new User(
        28,
        "popular",
        Arrays.asList(
            new PhoneNumber(1111111111L, "home"),
            new PhoneNumber(1111111111L, "apartment"),
            new PhoneNumber(2222222222L, null),
            new PhoneNumber(3333333333L, "mobile")),
        null));

    users.add(new User(30, null, Arrays.asList(new PhoneNumber(1111111111L, "home")), null));

    users.add(new User(31, null, Arrays.asList(new PhoneNumber(2222222222L, "business")), null));

    for (int i = 100; i < 200; i++) {
      Location location = null;
      if (i % 3 == 1) {
        location = new Location((double) i, (double) i * 2);
      }
      if (i % 3 == 2) {
        location = new Location((double) i, null);
      }
      users.add(new User(i, "p" + i, Arrays.asList(new PhoneNumber(i, "cell")), location));
    }

    return users;
  }

  private static File phonebookFile;
  private static List<User> users;

  @BeforeClass
  public static void setup() throws IOException {
    users = makeUsers();
    phonebookFile = PhoneBookWriter.writeToFile(users);
  }

  private static interface UserFilter {
    boolean keep(User u);
  }

  private static List<Group> getExpected(UserFilter f) {
    List<Group> expected = new ArrayList<Group>();
    for (User u : users) {
      if (f.keep(u)) {
        expected.add(PhoneBookWriter.groupFromUser(u));
      }
    }
    return expected;
  }

  private static void assertFilter(List<Group> found, UserFilter f) {
    List<Group> expected = getExpected(f);
    assertEquals(expected.size(), found.size());
    Iterator<Group> expectedIter = expected.iterator();
    Iterator<Group> foundIter = found.iterator();
    while (expectedIter.hasNext()) {
      assertEquals(expectedIter.next().toString(), foundIter.next().toString());
    }
  }

  private static void assertPredicate(FilterPredicate predicate, long... expectedIds) throws IOException {
    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(predicate));

    assertArrayEquals(
        Arrays.stream(expectedIds).boxed().toArray(),
        found.stream().map(f -> f.getLong("id", 0)).toArray(Long[]::new));
  }

  @Test
  public void testNoFilter() throws Exception {
    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.NOOP);
    assertFilter(found, new UserFilter() {
      @Override
      public boolean keep(User u) {
        return true;
      }
    });
  }

  @Test
  public void testAllFilter() throws Exception {
    BinaryColumn name = binaryColumn("name");

    FilterPredicate pred = eq(name, Binary.fromString("no matches"));

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(pred));
    assertEquals(new ArrayList<Group>(), found);
  }

  @Test
  public void testInFilter() throws Exception {
    BinaryColumn name = binaryColumn("name");

    HashSet<Binary> nameSet = new HashSet<>();
    nameSet.add(Binary.fromString("thing2"));
    nameSet.add(Binary.fromString("thing1"));
    for (int i = 100; i < 200; i++) {
      nameSet.add(Binary.fromString("p" + i));
    }
    FilterPredicate pred = in(name, nameSet);
    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(pred));

    List<String> expectedNames = new ArrayList<>();
    expectedNames.add("thing1");
    expectedNames.add("thing2");
    for (int i = 100; i < 200; i++) {
      expectedNames.add("p" + i);
    }
    expectedNames.add("dummy1");
    expectedNames.add("dummy2");
    expectedNames.add("dummy3");

    // validate that all the values returned by the reader fulfills the filter and there are no values left out,
    // i.e. "thing1", "thing2" and from "p100" to "p199" and nothing else.
    assertEquals(expectedNames.get(0), ((Group) (found.get(0))).getString("name", 0));
    assertEquals(expectedNames.get(1), ((Group) (found.get(1))).getString("name", 0));
    for (int i = 2; i < 102; i++) {
      assertEquals(expectedNames.get(i), ((Group) (found.get(i))).getString("name", 0));
    }
    assert (found.size() == 102);
  }

  @Test
  public void testArrayContains() throws Exception {
    assertPredicate(
        contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home"))), 27L, 28L, 30L);

    assertPredicate(
        contains(notEq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell"))),
        27L,
        28L,
        30L,
        31L);

    assertPredicate(contains(gt(longColumn("phoneNumbers.phone.number"), 1111111111L)), 20L, 27L, 28L, 31L);

    assertPredicate(contains(gtEq(longColumn("phoneNumbers.phone.number"), 1111111111L)), 20L, 27L, 28L, 30L, 31L);

    assertPredicate(contains(lt(longColumn("phoneNumbers.phone.number"), 105L)), 100L, 101L, 102L, 103L, 104L);

    assertPredicate(
        contains(ltEq(longColumn("phoneNumbers.phone.number"), 105L)), 100L, 101L, 102L, 103L, 104L, 105L);

    assertPredicate(
        contains(in(
            binaryColumn("phoneNumbers.phone.kind"),
            ImmutableSet.of(Binary.fromString("apartment"), Binary.fromString("home")))),
        27L,
        28L,
        30L);

    assertPredicate(
        contains(notIn(binaryColumn("phoneNumbers.phone.kind"), ImmutableSet.of(Binary.fromString("cell")))),
        27L,
        28L,
        30L,
        31L);
  }

  @Test
  public void testArrayDoesNotContains() throws Exception {
    assertPredicate(
        not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
        17L,
        18L,
        19L,
        20L,
        28L,
        30L,
        31L);

    // test composed not(contains())
    assertPredicate(
        and(
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home"))))),
        17L,
        18L,
        19L,
        20L,
        31L);

    assertPredicate(
        and(
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
            and(
                not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home")))),
                not(contains(
                    eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("business")))))),
        17L,
        18L,
        19L,
        20L);

    assertPredicate(
        or(
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home"))))),
        LongStream.concat(LongStream.of(17L, 18L, 19L, 20L, 28L, 30L, 31L), LongStream.range(100L, 200L))
            .toArray());

    assertPredicate(
        or(
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
            and(
                not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home")))),
                not(contains(
                    eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("mobile")))))),
        LongStream.concat(LongStream.of(17L, 18L, 19L, 20L, 28L, 30L, 31L), LongStream.range(100L, 200L))
            .toArray());

    // Test composed contains() with not(contains())
    assertPredicate(
        and(
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
            or(
                contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home"))),
                contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("business"))))),
        28L,
        30L,
        31L);

    assertPredicate(
        or(
            not(contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("cell")))),
            and(
                contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home"))),
                contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("apartment"))))),
        17L,
        18L,
        19L,
        20L,
        28L,
        30L,
        31L);
  }

  @Test
  public void testArrayContainsSimpleAndFilter() throws Exception {
    assertPredicate(
        and(
            contains(eq(longColumn("phoneNumbers.phone.number"), 1111111111L)),
            contains(eq(longColumn("phoneNumbers.phone.number"), 3333333333L))),
        28L);

    assertPredicate(
        and(
            contains(eq(longColumn("phoneNumbers.phone.number"), 1111111111L)),
            contains(eq(longColumn("phoneNumbers.phone.number"), -123L))) // Won't match
        );
  }

  @Test
  public void testArrayContainsNestedAndFilter() throws Exception {
    assertPredicate(
        and(
            contains(eq(longColumn("phoneNumbers.phone.number"), 1111111111L)),
            and(
                contains(eq(longColumn("phoneNumbers.phone.number"), 2222222222L)),
                contains(eq(longColumn("phoneNumbers.phone.number"), 3333333333L)))),
        28L);
  }

  @Test
  public void testArrayContainsSimpleOrFilter() throws Exception {
    assertPredicate(
        or(
            contains(eq(longColumn("phoneNumbers.phone.number"), 5555555555L)),
            contains(eq(longColumn("phoneNumbers.phone.number"), 2222222222L))),
        20L,
        27L,
        28L,
        31L);

    assertPredicate(
        or(
            contains(eq(longColumn("phoneNumbers.phone.number"), 5555555555L)),
            contains(eq(longColumn("phoneNumbers.phone.number"), -123L))), // Won't match
        20L);
  }

  @Test
  public void testArrayContainsNestedOrFilter() throws Exception {
    assertPredicate(
        or(
            contains(eq(longColumn("phoneNumbers.phone.number"), 5555555555L)),
            or(
                contains(eq(longColumn("phoneNumbers.phone.number"), -10000000L)), // Won't be matched
                contains(eq(longColumn("phoneNumbers.phone.number"), 2222222222L)))),
        20L,
        27L,
        28L,
        31L);
  }

  @Test
  public void testMapContains() throws Exception {
    // Test key predicate
    assertPredicate(contains(eq(binaryColumn("accounts.key_value.key"), Binary.fromString("business"))), 17L, 19L);

    // Test value predicate
    assertPredicate(contains(eq(doubleColumn("accounts.key_value.value"), 1000.0D)), 17L, 19L);
  }

  @Test
  public void testArrayContainsMixedColumns() throws Exception {
    assertPredicate(
        and(
            contains(eq(binaryColumn("phoneNumbers.phone.kind"), Binary.fromString("home"))),
            not(contains(eq(longColumn("phoneNumbers.phone.number"), 2222222222L)))),
        30L);
  }

  @Test
  public void testArraySizeRequiredColumn() throws Exception {
    assertPredicate(size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.EQ, 2), 27L);

    assertPredicate(size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.EQ, 4), 28L);

    assertPredicate(size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.GT, 1), 27L, 28L);

    assertPredicate(size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.GTE, 4), 28L);

    assertPredicate(size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.EQ, 0), 17L, 18L, 19L);

    assertPredicate(
        size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.LT, 2),
        LongStream.concat(LongStream.of(17L, 18L, 19L, 20L, 30L, 31L), LongStream.range(100, 200))
            .toArray());

    assertPredicate(
        size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.LTE, 2),
        LongStream.concat(LongStream.of(17L, 18L, 19L, 20L, 27L, 30L, 31L), LongStream.range(100, 200))
            .toArray());

    assertPredicate(
        not(size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.EQ, 1)),
        17L,
        18L,
        19L,
        27L,
        28L);

    assertPredicate(
        and(
            size(longColumn("phoneNumbers.phone.number"), Operators.Size.Operator.EQ, 0),
            size(binaryColumn("accounts.key_value.key"), Operators.Size.Operator.GT, 1)),
        17L,
        19L);
  }

  @Test
  public void testNameNotNull() throws Exception {
    BinaryColumn name = binaryColumn("name");

    FilterPredicate pred = notEq(name, null);

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(pred));

    assertFilter(found, new UserFilter() {
      @Override
      public boolean keep(User u) {
        return u.getName() != null;
      }
    });
  }

  public static class StartWithP extends UserDefinedPredicate<Binary> {

    @Override
    public boolean keep(Binary value) {
      if (value == null) {
        return false;
      }
      return value.toStringUsingUTF8().startsWith("p");
    }

    @Override
    public boolean canDrop(Statistics<Binary> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Binary> statistics) {
      return false;
    }
  }

  public static class SetInFilter extends UserDefinedPredicate<Long> implements Serializable {

    private HashSet<Long> hSet;

    public SetInFilter(HashSet<Long> phSet) {
      hSet = phSet;
    }

    @Override
    public boolean keep(Long value) {
      if (value == null) {
        return false;
      }

      return hSet.contains(value);
    }

    @Override
    public boolean canDrop(Statistics<Long> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Long> statistics) {
      return false;
    }
  }

  @Test
  public void testNameNotStartWithP() throws Exception {
    BinaryColumn name = binaryColumn("name");

    FilterPredicate pred = not(userDefined(name, StartWithP.class));

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(pred));

    assertFilter(found, new UserFilter() {
      @Override
      public boolean keep(User u) {
        return u.getName() == null || !u.getName().startsWith("p");
      }
    });
  }

  @Test
  public void testUserDefinedByInstance() throws Exception {
    LongColumn name = longColumn("id");

    final HashSet<Long> h = new HashSet<Long>();
    h.add(20L);
    h.add(27L);
    h.add(28L);

    FilterPredicate pred = userDefined(name, new SetInFilter(h));

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(pred));

    assertFilter(found, new UserFilter() {
      @Override
      public boolean keep(User u) {
        return u != null && h.contains(u.getId());
      }
    });
  }

  @Test
  public void testComplex() throws Exception {
    BinaryColumn name = binaryColumn("name");
    DoubleColumn lon = doubleColumn("location.lon");
    DoubleColumn lat = doubleColumn("location.lat");

    FilterPredicate pred = or(and(gt(lon, 150.0), notEq(lat, null)), eq(name, Binary.fromString("alice")));

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, FilterCompat.get(pred));

    assertFilter(found, new UserFilter() {
      @Override
      public boolean keep(User u) {
        String name = u.getName();
        Double lat = null;
        Double lon = null;
        if (u.getLocation() != null) {
          lat = u.getLocation().getLat();
          lon = u.getLocation().getLon();
        }

        return (lon != null && lon > 150.0 && lat != null) || "alice".equals(name);
      }
    });
  }
}
