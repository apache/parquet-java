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
package parquet.hadoop.thrift;

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumer;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.thrift.ParquetWriteProtocol;
import parquet.thrift.TBaseRecordConverter;
import parquet.thrift.ThriftReader;
import parquet.thrift.ThriftRecordConverter;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;
import com.twitter.elephantbird.thrift.test.TestMap;
import com.twitter.elephantbird.thrift.test.TestName;
import com.twitter.elephantbird.thrift.test.TestPerson;
import com.twitter.elephantbird.thrift.test.TestPhoneType;
import com.twitter.elephantbird.thrift.test.TestStructInMap;

public class TestParquetReadProtocol {

  @Test
  public void testReadEmpty() throws Exception {
    AddressBook expected = new AddressBook();
    validate(expected);
  }

  @Test
  public void testRead() throws Exception {
    List<Person> persons = Arrays.asList(
        new Person(
            new Name("john", "johson"),
            1,
            "john@johnson.org",
            Arrays.asList(new PhoneNumber("5555555555"))
            ),
        new Person(
            new Name("jack", "jackson"),
            2,
            "jack@jackson.org",
            Arrays.asList(new PhoneNumber("5555555556"))
            )
        );
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
    TestStructInMap testMap = new TestStructInMap("map_name", map);
    validate(testMap);
  }

  private <T extends TBase<?,?>> void validate(T expected) throws TException {
    @SuppressWarnings("unchecked")
    final Class<T> thriftClass = (Class<T>)expected.getClass();
    final MemPageStore memPageStore = new MemPageStore();
    final ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final MessageType schema = schemaConverter.convert(thriftClass);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    final MemColumnWriteStore columns = new MemColumnWriteStore(memPageStore, 10000);
    final RecordConsumer recordWriter = columnIO.getRecordWriter(columns);
    final StructType thriftType = schemaConverter.toStructType(thriftClass);
    ParquetWriteProtocol parquetWriteProtocol = new ParquetWriteProtocol(recordWriter, columnIO, thriftType);

    expected.write(parquetWriteProtocol);
    columns.flush();

    ThriftRecordConverter<T> converter = new TBaseRecordConverter<T>(thriftClass, schema, thriftType);
    final RecordReader<T> recordReader = columnIO.getRecordReader(memPageStore, converter);

    final T result = recordReader.read();

    assertEquals(expected, result);
  }

}
