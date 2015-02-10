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
package parquet.filter2.recordlevel;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.junit.BeforeClass;
import org.junit.Test;

import parquet.example.data.Group;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.filter2.predicate.Operators.DoubleColumn;
import parquet.filter2.predicate.Operators.LongColumn;
import parquet.filter2.predicate.Statistics;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.filter2.recordlevel.PhoneBookWriter.Location;
import parquet.filter2.recordlevel.PhoneBookWriter.PhoneNumber;
import parquet.filter2.recordlevel.PhoneBookWriter.User;
import parquet.io.api.Binary;

import static org.junit.Assert.assertEquals;
import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.doubleColumn;
import static parquet.filter2.predicate.FilterApi.longColumn;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.gt;
import static parquet.filter2.predicate.FilterApi.not;
import static parquet.filter2.predicate.FilterApi.notEq;
import static parquet.filter2.predicate.FilterApi.or;
import static parquet.filter2.predicate.FilterApi.userDefined;

public class TestRecordLevelFilters {

  public static List<User> makeUsers() {
    List<User> users = new ArrayList<User>();

    users.add(new User(17, null, null, null));

    users.add(new User(18, "bob", null, null));

    users.add(new User(19, "alice", new ArrayList<PhoneNumber>(), null));

    users.add(new User(20, "thing1", Arrays.asList(new PhoneNumber(5555555555L, null)), null));

    users.add(new User(27, "thing2", Arrays.asList(new PhoneNumber(1111111111L, "home")), null));

    users.add(new User(28, "popular", Arrays.asList(
        new PhoneNumber(1111111111L, "home"),
        new PhoneNumber(2222222222L, null),
        new PhoneNumber(3333333333L, "mobile")
    ), null));

    users.add(new User(30, null, Arrays.asList(new PhoneNumber(1111111111L, "home")), null));

    for (int i = 100; i < 200; i++) {
      Location location = null;
      if (i % 3 == 1) {
        location = new Location((double)i, (double)i*2);
      }
      if (i % 3 == 2) {
        location = new Location((double)i, null);
      }
      users.add(new User(i, "p" + i, Arrays.asList(new PhoneNumber(i, "cell")), location));
    }

    return users;
  }

  private static File phonebookFile;
  private static List<User> users;

  @BeforeClass
  public static void setup() throws IOException{
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
    while(expectedIter.hasNext()) {
      assertEquals(expectedIter.next().toString(), foundIter.next().toString());
    }
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
