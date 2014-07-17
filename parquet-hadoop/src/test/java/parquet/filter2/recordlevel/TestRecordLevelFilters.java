package parquet.filter2.recordlevel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import parquet.Optional;
import parquet.example.data.Group;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.filter2.predicate.Operators.DoubleColumn;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.filter2.recordlevel.PhoneBookWriter.Location;
import parquet.filter2.recordlevel.PhoneBookWriter.PhoneNumber;
import parquet.filter2.recordlevel.PhoneBookWriter.User;
import parquet.io.api.Binary;

import static org.junit.Assert.assertEquals;
import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.doubleColumn;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.gt;
import static parquet.filter2.predicate.FilterApi.not;
import static parquet.filter2.predicate.FilterApi.notEq;
import static parquet.filter2.predicate.FilterApi.or;
import static parquet.filter2.predicate.FilterApi.userDefined;

public class TestRecordLevelFilters {

  private static List<User> makeUsers() {
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
    List<Group> found = PhoneBookWriter.readFile(phonebookFile, Optional.<FilterPredicate>absent());
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

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, Optional.of(pred));
    assertEquals(new ArrayList<Group>(), found);
  }

  @Test
  public void testNameNotNull() throws Exception {
    BinaryColumn name = binaryColumn("name");

    FilterPredicate pred = notEq(name, null);

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, Optional.of(pred));

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
    public boolean canDrop(Binary min, Binary max) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Binary min, Binary max) {
      return false;
    }
  }

  @Test
  public void testNameNotStartWithP() throws Exception {
    BinaryColumn name = binaryColumn("name");

    FilterPredicate pred = not(userDefined(name, StartWithP.class));

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, Optional.of(pred));

    assertFilter(found, new UserFilter() {
      @Override
      public boolean keep(User u) {
        return u.getName() == null || !u.getName().startsWith("p");
      }
    });
  }

  @Test
  public void testComplex() throws Exception {
    BinaryColumn name = binaryColumn("name");
    DoubleColumn lon = doubleColumn("location.lon");
    DoubleColumn lat = doubleColumn("location.lat");

    FilterPredicate pred = or(and(gt(lon, 150.0), notEq(lat, null)), eq(name, Binary.fromString("alice")));

    List<Group> found = PhoneBookWriter.readFile(phonebookFile, Optional.of(pred));

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
