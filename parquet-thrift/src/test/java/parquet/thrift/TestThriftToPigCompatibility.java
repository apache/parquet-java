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
package parquet.thrift;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import thrift.test.OneOfEach;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.Test;

import parquet.io.ColumnIOFactory;
import parquet.io.ConverterConsumer;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumerLoggingWrapper;
import parquet.io.api.RecordConsumer;
import parquet.pig.PigSchemaConverter;
import parquet.pig.convert.TupleRecordMaterializer;
import parquet.schema.MessageType;
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

public class TestThriftToPigCompatibility {

  public void testMap() throws Exception {
    Map<String, String> map = new TreeMap<String, String>();
    map.put("foo", "bar");
    map.put("foo2", "bar2");
    TestMap testMap = new TestMap("map_name", map);
    validateSameTupleAsEB(testMap);
  }

  @Test
  public void testMapInSet() throws Exception {
    final Set<Map<String, String>> set = new HashSet<Map<String,String>>();
    final Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    set.add(map);
    TestMapInSet o = new TestMapInSet("top", set);
    validateSameTupleAsEB(o);
  }

  @Test
  public void testStructInMap() throws Exception {

    final Map<String, TestPerson> map = new HashMap<String, TestPerson>();
    map.put("foo", new TestPerson(new TestName("john", "johnson"), new HashMap<TestPhoneType, String>()));
    final Map<String, Integer> stringToIntMap = Collections.singletonMap("bar", 10);
    TestStructInMap testMap = new TestStructInMap("map_name", map, stringToIntMap);
    validateSameTupleAsEB(testMap);
  }

  @Test
  public void testProtocolEmptyAdressBook() throws Exception {

    AddressBook a = new AddressBook(new ArrayList<Person>());
    validateSameTupleAsEB(a);
  }

  @Test
  public void testProtocolAddressBook() throws Exception {
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
    validateSameTupleAsEB(a);
  }


  @Test
  public void testOneOfEach() throws Exception {
    OneOfEach a = new OneOfEach(
        true, false, (byte)8, (short)16, (int)32, (long)64, (double)1234, "string", "å", false,
        ByteBuffer.wrap("a".getBytes()), new ArrayList<Byte>(), new ArrayList<Short>(), new ArrayList<Long>());
    validateSameTupleAsEB(a);
  }

  @Test
  public void testStringList() throws Exception {
    final List<String> names = new ArrayList<String>();
    names.add("John");
    names.add("Jack");
    TestNameList o = new TestNameList("name", names);
    validateSameTupleAsEB(o);
  }

  /**
   * <ul> steps:
   * <li>Writes using the thrift mapping
   * <li>Reads using the pig mapping
   * <li>Use Elephant bird to convert from thrift to pig
   * <li>Check that both transformations give the same result
   * @param o the object to convert
   * @throws TException
   */
  public static <T extends TBase<?,?>> void validateSameTupleAsEB(T o) throws TException {
    final ThriftSchemaConverter thriftSchemaConverter = new ThriftSchemaConverter();
    @SuppressWarnings("unchecked")
    final Class<T> class1 = (Class<T>) o.getClass();
    final MessageType schema = thriftSchemaConverter.convert(class1);

    final StructType structType = thriftSchemaConverter.toStructType(class1);
    final ThriftToPig<T> thriftToPig = new ThriftToPig<T>(class1);
    final Schema pigSchema = thriftToPig.toSchema();
    final TupleRecordMaterializer tupleRecordConverter = new TupleRecordMaterializer(schema, pigSchema, true);
    RecordConsumer recordConsumer = new ConverterConsumer(tupleRecordConverter.getRootConverter(), schema);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    ParquetWriteProtocol p = new ParquetWriteProtocol(new RecordConsumerLoggingWrapper(recordConsumer), columnIO, structType);
    o.write(p);
    final Tuple t = tupleRecordConverter.getCurrentRecord();
    final Tuple expected = thriftToPig.getPigTuple(o);
    assertEquals(expected.toString(), t.toString());
    final MessageType filtered = new PigSchemaConverter().filter(schema, pigSchema);
    assertEquals(schema.toString(), filtered.toString());
  }
}
