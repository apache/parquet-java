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

import com.twitter.data.proto.tutorial.thrift.*;
import com.twitter.elephantbird.thrift.test.TestMapInSet;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.Test;
import parquet.thrift.test.Phone;
import parquet.thrift.test.StructWithExtraField;
import parquet.thrift.test.StructWithIndexStartsFrom4;
import parquet.thrift.test.compat.*;
import thrift.test.OneOfEach;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProtocolReadToWrite {

  @Test
  public void testOneOfEach() throws Exception {
    OneOfEach a = new OneOfEach(
        true, false, (byte)8, (short)16, (int)32, (long)64, (double)1234, "string", "å", false,
        ByteBuffer.wrap("a".getBytes()), new ArrayList<Byte>(), new ArrayList<Short>(), new ArrayList<Long>());
    writeReadCompare(a);
  }

  @Test
  public void testWriteRead() throws Exception {
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
    writeReadCompare(a);
  }

  @Test
  public void testEmptyStruct() throws Exception {
    AddressBook a = new AddressBook();
    writeReadCompare(a);
  }

  @Test
  public void testMapSet() throws Exception {
    final Set<Map<String, String>> set = new HashSet<Map<String, String>>();
    final Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    set.add(map);
    TestMapInSet a = new TestMapInSet("top", set);
    writeReadCompare(a);
  }

  private void writeReadCompare(TBase<?, ?> a)
          throws TException, InstantiationException, IllegalAccessException {
    ProtocolPipe[] pipes = {new ProtocolReadToWrite(), new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType((Class<TBase<?, ?>>)a.getClass()))};
    for (ProtocolPipe p : pipes) {
      final ByteArrayOutputStream in = new ByteArrayOutputStream();
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      a.write(protocol(in));
      p.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));
      TBase<?, ?> b = a.getClass().newInstance();
      b.read(protocol(new ByteArrayInputStream(out.toByteArray())));

      assertEquals(p.getClass().getSimpleName(), a, b);
    }
  }

  @Test
  public void testIncompatibleSchemaRecord() throws Exception {
    //handler will rethrow the exception for verifying purpose
    CountingErrorHandler countingHandler = new CountingErrorHandler();

    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType(AddressBook.class), countingHandler);

    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    OneOfEach a = new OneOfEach(
            true, false, (byte)8, (short)16, (int)32, (long)64, (double)1234, "string", "å", false,
            ByteBuffer.wrap("a".getBytes()), new ArrayList<Byte>(), new ArrayList<Short>(), new ArrayList<Long>());
    a.write(protocol(in));
    try {
    p.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));
    } catch (SkippableException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof DecodingSchemaMismatchException);
      assertTrue(cause.getMessage().contains("the data type does not match the expected thrift structure"));
      assertTrue(cause.getMessage().contains("got BOOL"));
    }
    assertEquals(0, countingHandler.recordCountOfMissingFields);
    assertEquals(0, countingHandler.fieldIgnoredCount);
  }

  /**
   * When enum value in data has an undefined index, it's considered as corrupted record and will be skipped.
   *
   * @throws Exception
   */
  @Test
  public void testEnumMissingSchema() throws Exception {
    CountingErrorHandler countingHandler = new CountingErrorHandler();
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType(StructWithEnum.class), countingHandler);
    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    StructWithMoreEnum enumDefinedInOldDefinition = new StructWithMoreEnum(NumberEnumWithMoreValue.THREE);
    StructWithMoreEnum extraEnumDefinedInNewDefinition = new StructWithMoreEnum(NumberEnumWithMoreValue.FOUR);
    enumDefinedInOldDefinition.write(protocol(in));
    extraEnumDefinedInNewDefinition.write(protocol(in));
    try {
      p.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));
    } catch (SkippableException e) {
      Throwable cause = e.getCause();
      assertTrue(cause instanceof DecodingSchemaMismatchException);
      assertTrue(cause.getMessage().contains("can not find index 4 in enum"));
    }
    assertEquals(0, countingHandler.recordCountOfMissingFields);
    assertEquals(0, countingHandler.fieldIgnoredCount);
  }

  /**
   * When data contains extra field, it should notify the handler and read the data with extra field dropped
   * @throws Exception
   */
  @Test
  public void testMissingFieldHandling() throws Exception {

    CountingErrorHandler countingHandler = new CountingErrorHandler() {
      @Override
      public void handleFieldIgnored(TField field) {
        assertEquals(field.id, 4);
        fieldIgnoredCount++;
      }
    };
    BufferedProtocolReadToWrite structForRead = new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType(StructV3.class), countingHandler);

    //Data has an extra field of type struct
    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    StructV4WithExtracStructField dataWithNewSchema = new StructV4WithExtracStructField("name");
    dataWithNewSchema.setAge("10");
    dataWithNewSchema.setGender("male");
    StructV3 structV3 = new StructV3("name");
    structV3.setAge("10");
    dataWithNewSchema.setAddedStruct(structV3);
    dataWithNewSchema.write(protocol(in));

    //read using the schema that doesn't have the extra field
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    structForRead.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));

    //record will be read without extra field
    assertEquals(1, countingHandler.recordCountOfMissingFields);
    assertEquals(1, countingHandler.fieldIgnoredCount);

    StructV4WithExtracStructField b = StructV4WithExtracStructField.class.newInstance();
    b.read(protocol(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(dataWithNewSchema.getName(), b.getName());
    assertEquals(dataWithNewSchema.getAge(), b.getAge());
    assertEquals(dataWithNewSchema.getGender(), b.getGender());
    assertEquals(null, b.getAddedStruct());
  }

  @Test
  public void TestExtraFieldWhenFieldIndexIsNotStartFromZero() throws Exception {
    CountingErrorHandler countingHandler = new CountingErrorHandler() {
      @Override
      public void handleFieldIgnored(TField field) {
        assertEquals(3, field.id);
        fieldIgnoredCount++;
      }
    };

    BufferedProtocolReadToWrite structForRead = new BufferedProtocolReadToWrite(new ThriftSchemaConverter().toStructType(StructWithIndexStartsFrom4.class), countingHandler);

    //Data has an extra field of type struct
    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    StructWithExtraField dataWithNewExtraField = new StructWithExtraField(new Phone("111", "222"), new Phone("333", "444"));
    dataWithNewExtraField.write(protocol(in));

    //read using the schema that doesn't have the extra field
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    structForRead.readOne(protocol(new ByteArrayInputStream(in.toByteArray())), protocol(out));

    assertEquals(1, countingHandler.recordCountOfMissingFields);
    assertEquals(1, countingHandler.fieldIgnoredCount);
  }

  private TCompactProtocol protocol(OutputStream to) {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) {
    return new TCompactProtocol(new TIOStreamTransport(from));
  }

  class CountingErrorHandler extends FieldIgnoredHandler {
    int fieldIgnoredCount = 0;
    int recordCountOfMissingFields = 0;

    @Override
    public void handleRecordHasFieldIgnored() {
      recordCountOfMissingFields++;
    }

    @Override
    public void handleFieldIgnored(TField field) {
      fieldIgnoredCount++;
    }
  }
}
