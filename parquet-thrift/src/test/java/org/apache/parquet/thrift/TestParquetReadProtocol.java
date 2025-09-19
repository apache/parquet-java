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
package org.apache.parquet.thrift;

import static com.twitter.data.proto.tutorial.thrift.PhoneType.MOBILE;
import static org.junit.Assert.assertEquals;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.elephantbird.thrift.test.TestMap;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestNameList;
import com.twitter.elephantbird.thrift.test.TestNameSet;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.thrift.test.TestStructInMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.impl.ColumnWriteStoreV1;
import org.apache.parquet.column.page.mem.MemPageStore;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import thrift.test.OneOfEach;

public class TestParquetReadProtocol {
  private static final Logger LOG = LoggerFactory.getLogger(TestParquetReadProtocol.class);

  @Test
  public void testList() throws TException {
    final List<String> names = new ArrayList<String>();
    names.add("John");
    names.add("Jack");
    final TestNameList o = new TestNameList("name", names);
    validate(o);
  }

  @Test
  public void testSet() throws TException {
    final Set<String> names = new HashSet<String>();
    names.add("John");
    names.add("Jack");
    final TestNameSet o = new TestNameSet("name", names);
    validate(o);
  }

  @Test
  public void testReadEmpty() throws Exception {
    AddressBook expected = new AddressBook();
    validate(expected);
  }

  @Test
  public void testOneOfEach() throws TException {
    final List<Byte> bytes = new ArrayList<Byte>();
    bytes.add((byte) 1);
    final List<Short> shorts = new ArrayList<Short>();
    shorts.add((short) 1);
    final List<Long> longs = new ArrayList<Long>();
    longs.add((long) 1);
    OneOfEach a = new OneOfEach(
        true,
        false,
        (byte) 8,
        (short) 16,
        (int) 32,
        (long) 64,
        (double) 1234,
        "string",
        "å",
        false,
        ByteBuffer.wrap("a".getBytes()),
        bytes,
        shorts,
        longs);
    validate(a);
  }

  @Test
  public void testRead() throws Exception {
    final PhoneNumber phoneNumber = new PhoneNumber("5555555555");
    phoneNumber.type = MOBILE;
    List<Person> persons = Arrays.asList(
        new Person(new Name("john", "johson"), 1, "john@johnson.org", Arrays.asList(phoneNumber)),
        new Person(
            new Name("jack", "jackson"),
            2,
            "jack@jackson.org",
            Arrays.asList(new PhoneNumber("5555555556"))));
    AddressBook expected = new AddressBook(persons);
    validate(expected);
  }

  @Test
  public void testMap() throws Exception {
    final Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    TestMap testMap = new TestMap("map_name", map);
    validate(testMap);
  }

  @Test
  public void testStructInMap() throws Exception {
    final Map<String, TestPerson> map = new HashMap<String, TestPerson>();
    map.put("foo", new TestPerson(new TestName("john", "johnson"), new HashMap<TestPhoneType, String>()));
    final Map<String, Integer> stringToIntMap = Collections.singletonMap("bar", 10);
    TestStructInMap testMap = new TestStructInMap("map_name", map, stringToIntMap);
    validate(testMap);
  }

  private <T extends TBase<?, ?>> void validate(T expected) throws TException {
    @SuppressWarnings("unchecked")
    final Class<T> thriftClass = (Class<T>) expected.getClass();
    final MemPageStore memPageStore = new MemPageStore(1);
    final ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final MessageType schema = schemaConverter.convert(thriftClass);
    LOG.info("{}", schema);
    final MessageColumnIO columnIO = new ColumnIOFactory(true).getColumnIO(schema);
    final ColumnWriteStoreV1 columns = new ColumnWriteStoreV1(
        memPageStore,
        ParquetProperties.builder()
            .withPageSize(10000)
            .withDictionaryEncoding(false)
            .build());
    final RecordConsumer recordWriter = columnIO.getRecordWriter(columns);
    final StructType thriftType = schemaConverter.toStructType(thriftClass);
    ParquetWriteProtocol parquetWriteProtocol = new ParquetWriteProtocol(recordWriter, columnIO, thriftType);

    expected.write(parquetWriteProtocol);
    recordWriter.flush();
    columns.flush();

    ThriftRecordConverter<T> converter = new TBaseRecordConverter<T>(thriftClass, schema, thriftType, new Configuration());
    final RecordReader<T> recordReader = columnIO.getRecordReader(memPageStore, converter);

    final T result = recordReader.read();

    assertEquals(expected, result);
  }
}
