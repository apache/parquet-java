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

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.apache.parquet.thrift.test.Phone;
import org.apache.parquet.thrift.test.StructWithExtraField;
import org.apache.parquet.thrift.test.StructWithIndexStartsFrom4;
import org.apache.parquet.thrift.test.compat.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestProtocolReadToWrite {

  @Test
  public void testUnrecognizedUnionMemberSchema() throws Exception {
    CountingErrorHandler countingHandler = new CountingErrorHandler();
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(ThriftSchemaConverter.toStructType(StructWithUnionV1.class), countingHandler);
    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    StructWithUnionV1 validUnion = new StructWithUnionV1("a valid struct", UnionV1.aLong(new ALong(17L)));
    StructWithUnionV2 invalidUnion = new StructWithUnionV2("a struct with new union member",
        UnionV2.aNewBool(new ABool(true)));

    validUnion.write(protocol(in));
    invalidUnion.write(protocol(in));

    ByteArrayInputStream baos = new ByteArrayInputStream(in.toByteArray());

    // first one should not throw
    p.readOne(protocol(baos), protocol(out));

    try {
      p.readOne(protocol(baos), protocol(out));
      fail("this should throw");
    } catch (SkippableException e) {
      Throwable cause = e.getCause();
      assertEquals(DecodingSchemaMismatchException.class, cause.getClass());
      assertTrue(cause.getMessage().startsWith("Unrecognized union member with id: 3 for struct:"));
    }
    assertEquals(0, countingHandler.recordCountOfMissingFields);
    assertEquals(0, countingHandler.fieldIgnoredCount);
  }

  @Test
  public void testUnionWithExtraOrNoValues() throws Exception {
    CountingErrorHandler countingHandler = new CountingErrorHandler();
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(ThriftSchemaConverter.toStructType(StructWithUnionV2.class), countingHandler);
    ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    StructWithUnionV2 validUnion = new StructWithUnionV2("a valid struct", UnionV2.aLong(new ALong(17L)));

    StructWithAStructThatLooksLikeUnionV2 allMissing = new StructWithAStructThatLooksLikeUnionV2("all missing",
        new AStructThatLooksLikeUnionV2());

    AStructThatLooksLikeUnionV2 extra = new AStructThatLooksLikeUnionV2();
    extra.setALong(new ALong(18L));
    extra.setANewBool(new ABool(false));

    StructWithAStructThatLooksLikeUnionV2 hasExtra = new StructWithAStructThatLooksLikeUnionV2("has extra",
        new AStructThatLooksLikeUnionV2(extra));

    validUnion.write(protocol(in));
    allMissing.write(protocol(in));

    ByteArrayInputStream baos = new ByteArrayInputStream(in.toByteArray());

    // first one should not throw
    p.readOne(protocol(baos), protocol(out));

    try {
      p.readOne(protocol(baos), protocol(out));
      fail("this should throw");
    } catch (SkippableException e) {
      Throwable cause = e.getCause();
      assertEquals(DecodingSchemaMismatchException.class, cause.getClass());
      assertTrue(cause.getMessage().startsWith("Cannot write a TUnion with no set value in"));
    }
    assertEquals(0, countingHandler.recordCountOfMissingFields);
    assertEquals(0, countingHandler.fieldIgnoredCount);

    in = new ByteArrayOutputStream();
    validUnion.write(protocol(in));
    hasExtra.write(protocol(in));

    baos = new ByteArrayInputStream(in.toByteArray());

    // first one should not throw
    p.readOne(protocol(baos), protocol(out));

    try {
      p.readOne(protocol(baos), protocol(out));
      fail("this should throw");
    } catch (SkippableException e) {
      Throwable cause = e.getCause();
      assertEquals(DecodingSchemaMismatchException.class, cause.getClass());
      assertTrue(cause.getMessage().startsWith("Cannot write a TUnion with more than 1 set value in"));
    }
    assertEquals(0, countingHandler.recordCountOfMissingFields);
    assertEquals(0, countingHandler.fieldIgnoredCount);
  }

  @Test
  public void testUnionWithStructWithUnknownField() throws Exception {
    CountingErrorHandler countingHandler = new CountingErrorHandler();
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(ThriftSchemaConverter.toStructType(UnionV3.class), countingHandler);
    ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    UnionV3 validUnion = UnionV3.aStruct(new StructV1("a valid struct"));
    StructV2 structV2 = new StructV2("a valid struct");
    structV2.setAge("a valid age");
    UnionThatLooksLikeUnionV3 unionWithUnknownStructField = UnionThatLooksLikeUnionV3.aStruct(structV2);

    validUnion.write(protocol(in));
    unionWithUnknownStructField.write(protocol(in));

    ByteArrayInputStream baos = new ByteArrayInputStream(in.toByteArray());

    // both should not throw
    p.readOne(protocol(baos), protocol(out));
    p.readOne(protocol(baos), protocol(out));

    assertEquals(1, countingHandler.recordCountOfMissingFields);
    assertEquals(1, countingHandler.fieldIgnoredCount);

    in = new ByteArrayOutputStream();
    validUnion.write(protocol(in));
    unionWithUnknownStructField.write(protocol(in));

    baos = new ByteArrayInputStream(in.toByteArray());

    // both should not throw
    p.readOne(protocol(baos), protocol(out));
    p.readOne(protocol(baos), protocol(out));

    assertEquals(2, countingHandler.recordCountOfMissingFields);
    assertEquals(2, countingHandler.fieldIgnoredCount);
  }

  /**
   * When enum value in data has an undefined index, it's considered as corrupted record and will be skipped.
   *
   * @throws Exception
   */
  @Test
  public void testEnumMissingSchema() throws Exception {
    CountingErrorHandler countingHandler = new CountingErrorHandler();
    BufferedProtocolReadToWrite p = new BufferedProtocolReadToWrite(ThriftSchemaConverter.toStructType(StructWithEnum.class), countingHandler);
    final ByteArrayOutputStream in = new ByteArrayOutputStream();
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    StructWithMoreEnum enumDefinedInOldDefinition = new StructWithMoreEnum(NumberEnumWithMoreValue.THREE);
    StructWithMoreEnum extraEnumDefinedInNewDefinition = new StructWithMoreEnum(NumberEnumWithMoreValue.FOUR);
    enumDefinedInOldDefinition.write(protocol(in));
    extraEnumDefinedInNewDefinition.write(protocol(in));

    ByteArrayInputStream baos = new ByteArrayInputStream(in.toByteArray());

    // first should not throw
    p.readOne(protocol(baos), protocol(out));

    try {
      p.readOne(protocol(baos), protocol(out));
      fail("this should throw");
    } catch (SkippableException e) {
      Throwable cause = e.getCause();
      assertEquals(DecodingSchemaMismatchException.class, cause.getClass());
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
    BufferedProtocolReadToWrite structForRead = new BufferedProtocolReadToWrite(ThriftSchemaConverter.toStructType(StructV3.class), countingHandler);

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

    BufferedProtocolReadToWrite structForRead = new BufferedProtocolReadToWrite(ThriftSchemaConverter.toStructType(StructWithIndexStartsFrom4.class), countingHandler);

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

  private TCompactProtocol protocol(OutputStream to) throws TTransportException {
    return new TCompactProtocol(new TIOStreamTransport(to));
  }

  private TCompactProtocol protocol(InputStream from) throws TTransportException {
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
