/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.thrift;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.Test;

import parquet.Log;
import parquet.io.ColumnIOFactory;
import parquet.io.ExpectationValidatingRecordConsumer;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumerLoggingWrapper;
import parquet.pig.PigSchemaConverter;
import parquet.pig.TupleWriteSupport;
import parquet.schema.MessageType;
import parquet.thrift.ParquetWriteProtocol;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.data.proto.tutorial.thrift.PhoneType;
import com.twitter.elephantbird.pig.util.ThriftToPig;
import com.twitter.elephantbird.thrift.test.TestMap;
import com.twitter.elephantbird.thrift.test.TestMapInSet;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestNameList;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.thrift.test.TestStructInMap;


public class TestParquetWriteProtocol {
  private static final Log LOG = Log.getLog(TestParquetWriteProtocol.class);
  @Test
  public void testMap() throws Exception {
    String[] expectations = {
        "startMessage()",
         "startField(name, 0)",
          "addBinary(map_name)",
         "endField(name, 0)",
         "startField(names, 1)",
          "startGroup()",
           "startField(map, 0)",
            "startGroup()",
             "startField(key, 0)",
              "addBinary(foo)",
             "endField(key, 0)",
             "startField(value, 1)",
              "addBinary(bar)",
             "endField(value, 1)",
            "endGroup()",
            "startGroup()",
             "startField(key, 0)",
              "addBinary(foo2)",
             "endField(key, 0)",
             "startField(value, 1)",
              "addBinary(bar2)",
             "endField(value, 1)",
            "endGroup()",
           "endField(map, 0)",
          "endGroup()",
         "endField(names, 1)",
        "endMessage()"
    };

    final Map<String, String> map = new TreeMap<String, String>();
    map.put("foo", "bar");
    map.put("foo2", "bar2");
    TestMap testMap = new TestMap("map_name", map);
    validatePig(expectations, testMap);
    validateThrift(expectations, testMap);
  }

  @Test
  public void testMapInSet() throws Exception {
    String[] pigExpectations = {
        "startMessage()",
         "startField(name, 0)",
          "addBinary(top)",
         "endField(name, 0)",
         "startField(names, 1)", // set: optional field
          "startGroup()",
           "startField(t, 0)", // repeated field
            "startGroup()",
             "startField(names_tuple, 0)", // map: optional field
              "startGroup()",
               "startField(map, 0)", // repeated field
                "startGroup()",
                 "startField(key, 0)", // key
                  "addBinary(foo)",
                 "endField(key, 0)",
                 "startField(value, 1)", // value
                  "addBinary(bar)",
                 "endField(value, 1)",
                "endGroup()",
               "endField(map, 0)",
              "endGroup()",
             "endField(names_tuple, 0)",
            "endGroup()",
           "endField(t, 0)",
          "endGroup()",
         "endField(names, 1)",
        "endMessage()"
    };

    final Set<Map<String, String>> set = new HashSet<Map<String,String>>();
    final Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    set.add(map);
    TestMapInSet o = new TestMapInSet("top", set);
    validatePig(pigExpectations, o);

    // TODO: test that the pig reader can figure out the extra nesting

    String[] expectationsThrift = {
        "startMessage()",
         "startField(name, 0)",
          "addBinary(top)",
         "endField(name, 0)",
         "startField(names, 1)", // set: optional field
          "startGroup()",
           "startField(names_tuple, 0)", // map: optional field
            "startGroup()",
             "startField(map, 0)", // repeated field
              "startGroup()",
               "startField(key, 0)", // key
                "addBinary(foo)",
               "endField(key, 0)",
               "startField(value, 1)", // value
                "addBinary(bar)",
               "endField(value, 1)",
              "endGroup()",
             "endField(map, 0)",
            "endGroup()",
           "endField(names_tuple, 0)",
          "endGroup()",
         "endField(names, 1)",
        "endMessage()"
    };
    validateThrift(expectationsThrift, o);
  }

  @Test
  public void testNameList() throws TException {
    final List<String> names = new ArrayList<String>();
    names.add("John");
    names.add("Jack");
    final TestNameList o = new TestNameList("name", names);

    // TODO: test that the pig reader can figure out the extra nesting

    String[] pigExpectations = {
        "startMessage()",
         "startField(name, 0)",
          "addBinary(name)",
         "endField(name, 0)",
         "startField(names, 1)",
          "startGroup()",
           "startField(t, 0)",
             "startGroup()",
               "startField(names_tuple, 0)",
                "addBinary(John)",
               "endField(names_tuple, 0)",
              "endGroup()",
              "startGroup()",
               "startField(names_tuple, 0)",
                "addBinary(Jack)",
               "endField(names_tuple, 0)",
             "endGroup()",
           "endField(t, 0)",
          "endGroup()",
         "endField(names, 1)",
        "endMessage()"};
    validatePig(pigExpectations, o);

    String[] expectations = {
        "startMessage()",
         "startField(name, 0)",
          "addBinary(name)",
         "endField(name, 0)",
         "startField(names, 1)",
          "startGroup()",
           "startField(names_tuple, 0)",
            "addBinary(John)",
            "addBinary(Jack)",
           "endField(names_tuple, 0)",
          "endGroup()",
         "endField(names, 1)",
        "endMessage()"};
    validateThrift(expectations, o);
  }

  @Test
  public void testStructInMap() throws Exception {
    String[] expectations = {
        "startMessage()",
        "startField(name, 0)",
        "addBinary(map_name)",
        "endField(name, 0)",
        "startField(names, 1)",
        "startGroup()",
        "startField(map, 0)",
        "startGroup()",
        "startField(key, 0)",
        "addBinary(foo)",
        "endField(key, 0)",
        "startField(value, 1)",
        "startGroup()",
        "startField(name, 0)",
        "startGroup()",
        "startField(first_name, 0)",
        "addBinary(john)",
        "endField(first_name, 0)",
        "startField(last_name, 1)",
        "addBinary(johnson)",
        "endField(last_name, 1)",
        "endGroup()",
        "endField(name, 0)",
        "startField(phones, 1)",
        "startGroup()",
        "endGroup()",
        "endField(phones, 1)",
        "endGroup()",
        "endField(value, 1)",
        "endGroup()",
        "endField(map, 0)",
        "endGroup()",
        "endField(names, 1)",
        "endMessage()"
    };

    final Map<String, TestPerson> map = new HashMap<String, TestPerson>();
    map.put("foo", new TestPerson(new TestName("john", "johnson"), new HashMap<TestPhoneType, String>()));
    TestStructInMap testMap = new TestStructInMap("map_name", map);
    validatePig(expectations, testMap);
    validateThrift(expectations, testMap);
  }

  @Test
  public void testProtocolEmptyAdressBook() throws Exception {
    String[] expectations = {
        "startMessage()",
        "startField(persons, 0)",
        "startGroup()",
        "endGroup()",
        "endField(persons, 0)",
        "endMessage()"
    };
    AddressBook a = new AddressBook(new ArrayList<Person>());
    validatePig(expectations, a);
    validateThrift(expectations, a);
  }

  @Test
  public void testProtocolAddressBook() throws Exception {
    String[] expectations = {
        "startMessage()", // AddressBook
         "startField(persons, 0)", // LIST: optional
          "startGroup()",
           "startField(t, 0)", // repeated: Person
            "startGroup()",
             "startField(name, 0)",
              "startGroup()", // Name
               "startField(first_name, 0)",
                "addBinary(Bob)",
               "endField(first_name, 0)",
               "startField(last_name, 1)",
                "addBinary(Roberts)",
               "endField(last_name, 1)",
              "endGroup()",
             "endField(name, 0)",
             "startField(id, 1)",
              "addInt(1)",
             "endField(id, 1)",
             "startField(email, 2)",
              "addBinary(bob@roberts.com)",
             "endField(email, 2)",
             "startField(phones, 3)",
              "startGroup()",
               "startField(t, 0)",
                "startGroup()",
                 "startField(number, 0)",
                  "addBinary(555 999 9999)",
                 "endField(number, 0)",
                "endGroup()",
                "startGroup()",
                 "startField(number, 0)",
                  "addBinary(555 999 9998)",
                 "endField(number, 0)",
                 "startField(type, 1)",
                  "addBinary(HOME)",
                 "endField(type, 1)",
                "endGroup()",
               "endField(t, 0)",
              "endGroup()",
             "endField(phones, 3)",
            "endGroup()",
            "startGroup()",
             "startField(name, 0)",
              "startGroup()",
               "startField(first_name, 0)",
                "addBinary(Dick)",
               "endField(first_name, 0)",
               "startField(last_name, 1)",
                "addBinary(Richardson)",
               "endField(last_name, 1)",
              "endGroup()",
             "endField(name, 0)",
             "startField(id, 1)",
              "addInt(2)",
             "endField(id, 1)",
             "startField(email, 2)",
              "addBinary(dick@richardson.com)",
             "endField(email, 2)",
             "startField(phones, 3)",
              "startGroup()",
               "startField(t, 0)",
                "startGroup()",
                 "startField(number, 0)",
                  "addBinary(555 999 9997)",
                 "endField(number, 0)",
                "endGroup()",
                "startGroup()",
                "startField(number, 0)",
                 "addBinary(555 999 9996)",
                "endField(number, 0)",
               "endGroup()",
              "endField(t, 0)",
             "endGroup()",
            "endField(phones, 3)",
           "endGroup()",
          "endField(t, 0)",
         "endGroup()",
        "endField(persons, 0)",
       "endMessage()"
    };
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
    validatePig(expectations, a);
    // naming conventions are slightly different for the bag inner tuple. The reader should ignore this.
    String[] expectationsThrift = Arrays.copyOf(expectations, expectations.length, String[].class);
    expectationsThrift[3] = "startField(persons_tuple, 0)";
    expectationsThrift[23] = "startField(phones_tuple, 0)";
    expectationsThrift[37] = "endField(phones_tuple, 0)";
    expectationsThrift[60] = "startField(phones_tuple, 0)";
    expectationsThrift[71] = "endField(phones_tuple, 0)";
    expectationsThrift[75] = "endField(persons_tuple, 0)";
    validateThrift(expectationsThrift, a);
  }

  private void validateThrift(String[] expectations, TBase<?, ?> a)
      throws TException {
    final ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter();
//      System.out.println(a);
    final Class<TBase<?,?>> class1 = (Class<TBase<?,?>>)a.getClass();
    final MessageType schema = thriftSchemaConverter.convert(class1);
    LOG.info(schema);
    final StructType structType = thriftSchemaConverter.toStructType(class1);
    ExpectationValidatingRecordConsumer recordConsumer = new ExpectationValidatingRecordConsumer(new ArrayDeque<String>(Arrays.asList(expectations)));
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    ParquetWriteProtocol p = new ParquetWriteProtocol(new RecordConsumerLoggingWrapper(recordConsumer), columnIO, structType);
    a.write(p);
  }

  private MessageType validatePig(String[] expectations, TBase<?, ?> a) {
    ThriftToPig<TBase<?,?>> thriftToPig = new ThriftToPig(a.getClass());
    ExpectationValidatingRecordConsumer recordConsumer = new ExpectationValidatingRecordConsumer(new ArrayDeque<String>(Arrays.asList(expectations)));
    Schema pigSchema = thriftToPig.toSchema();
    LOG.info(pigSchema);
    MessageType schema = new PigSchemaConverter().convert(pigSchema);
    LOG.info(schema);
    TupleWriteSupport tupleWriteSupport = new TupleWriteSupport(schema, pigSchema);
    tupleWriteSupport.init(null);
    tupleWriteSupport.prepareForWrite(recordConsumer);
    final Tuple pigTuple = thriftToPig.getPigTuple(a);
    LOG.info(pigTuple);
    tupleWriteSupport.write(pigTuple);
    return schema;
  }

}
