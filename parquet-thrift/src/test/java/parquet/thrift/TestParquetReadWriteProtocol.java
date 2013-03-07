package parquet.thrift;

import static junit.framework.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import thrift.test.OneOfEach;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.thrift.test.TestMapInSet;

public class TestParquetReadWriteProtocol {

  @Test
  public void testOneOfEach() throws TException {
    OneOfEach a = new OneOfEach(
        true, false, (byte)8, (short)16, (int)32, (long)64, (double)1234, "string", "å", false,
        ByteBuffer.wrap("a".getBytes()), new ArrayList<Byte>(), new ArrayList<Short>(), new ArrayList<Long>());
    OneOfEach b = new OneOfEach();
    writeReadCompare(a, b);
  }

  @Test
  public void testWriteRead() throws TException {
    ArrayList<Person> persons = new ArrayList<Person>();
    final PhoneNumber phoneNumber = new PhoneNumber("555 999 9998");
    phoneNumber.type = PhoneType.HOME;
    persons.add(
        new Person(
            new Name("Bob", "Roberts"),
            1,
            "bob@roberts.com",
            Arrays.asList(new PhoneNumber("555 999 9999"), phoneNumber)));
    persons.add(
        new Person(
            new Name("Dick", "Richardson"),
            2,
            "dick@richardson.com",
            Arrays.asList(new PhoneNumber("555 999 9997"), new PhoneNumber("555 999 9996"))));
    AddressBook a = new AddressBook(persons);
    final AddressBook b = new AddressBook();

    writeReadCompare(a, b);
  }

  @Test
  public void testMapSet() throws TException {
    final Set<Map<String, String>> set = new HashSet<Map<String,String>>();
    final Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    set.add(map);
    TestMapInSet a = new TestMapInSet("top", set);
    TestMapInSet b = new TestMapInSet();
    writeReadCompare(a, b);
  }

  private void writeReadCompare(TBase<?,?> a, TBase<?,?> b)
      throws TException {
    final ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
    final ByteArrayOutputStream baos = baos2;
    a.write(protocol(baos));
    new ParquetReadToWriteProtocol().readOne(protocol(new ByteArrayInputStream(baos.toByteArray())), protocol(baos2));
    b.read(protocol(new ByteArrayInputStream(baos2.toByteArray())));

    assertEquals(a, b);
  }

  private TCompactProtocol protocol(OutputStream to) {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) {
    return new TCompactProtocol(new TIOStreamTransport(from));
  }
}
