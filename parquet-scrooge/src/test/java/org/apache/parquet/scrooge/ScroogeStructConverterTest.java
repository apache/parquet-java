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
package org.apache.parquet.scrooge;

import org.junit.Test;

import org.apache.parquet.scrooge.test.AddressWithStreetWithDefaultRequirement;
import org.apache.parquet.scrooge.test.TestFieldOfEnum;
import org.apache.parquet.scrooge.test.TestListPrimitive;
import org.apache.parquet.scrooge.test.TestMapComplex;
import org.apache.parquet.scrooge.test.TestMapPrimitiveKey;
import org.apache.parquet.scrooge.test.TestMapPrimitiveValue;
import org.apache.parquet.scrooge.test.TestOptionalMap;
import org.apache.parquet.scrooge.test.TestPersonWithAllInformation;
import org.apache.parquet.scrooge.test.TestSetPrimitive;
import org.apache.parquet.scrooge.test.TestUnion;
import org.apache.parquet.scrooge.test.StringAndBinary;
import org.apache.parquet.thrift.ThriftSchemaConverter;
import org.apache.parquet.thrift.struct.ThriftType;
import static org.junit.Assert.assertEquals;

/**
 * Test convert scrooge schema to Parquet Schema
 */
public class ScroogeStructConverterTest {
  @Test
  public void testConvertPrimitiveMapKey() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeStructConverter().convert(TestMapPrimitiveKey.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestMapPrimitiveKey.class);
    assertEquals(expected,scroogeMap);
  }

  @Test
  public void testBinary() throws Exception {
    ThriftType.StructType scroogeBinary = new ScroogeStructConverter().convert(StringAndBinary.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.StringAndBinary.class);
    assertEquals(expected, scroogeBinary);
  }

  @Test
  public void testUnion() throws Exception {
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestUnion.class);
    ThriftType.StructType scroogeUnion = new ScroogeStructConverter().convert(TestUnion.class);
    assertEquals(expected, scroogeUnion);
  }

  @Test
  public void testConvertPrimitiveMapValue() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeStructConverter().convert(TestMapPrimitiveValue.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestMapPrimitiveValue.class);
    assertEquals(expected,scroogeMap);
  }

  @Test
  public void testConvertPrimitiveList() throws Exception{
    ThriftType.StructType scroogeList = new ScroogeStructConverter().convert(TestListPrimitive.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestListPrimitive.class);
    assertEquals(expected, scroogeList);
  }

  @Test
     public void testConvertPrimitiveSet() throws Exception{
    ThriftType.StructType scroogeList = new ScroogeStructConverter().convert(TestSetPrimitive.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestSetPrimitive.class);
    assertEquals(expected, scroogeList);
  }

  @Test
  public void testConvertEnum() throws Exception{
    ThriftType.StructType scroogeList = new ScroogeStructConverter().convert(TestFieldOfEnum.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestFieldOfEnum.class);
    assertEquals(expected, scroogeList);
  }

  @Test
  public void testMapComplex() throws Exception{
    ThriftType.StructType scroogePerson = new ScroogeStructConverter().convert(TestMapComplex.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestMapComplex.class);
    assertEquals(expected, scroogePerson);
  }

  @Test
  public void testConvertStruct() throws Exception{
    ThriftType.StructType scroogePerson = new ScroogeStructConverter().convert(TestPersonWithAllInformation.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestPersonWithAllInformation.class);
    assertEquals(expected, scroogePerson);
  }

/**
 * TODO: DEFAULT requirement can not be identified, since scrooge does not store the requirement type in generated class
 * Current solution uses reflection based on following rules:
 * if the getter returns option, then it's optional, otherwise it's required
 */
  @Test
  public void testDefaultFields() throws Exception{
    ThriftType.StructType scroogePerson = new ScroogeStructConverter().convert(AddressWithStreetWithDefaultRequirement.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.AddressWithStreetWithDefaultRequirement.class);
    assertEquals(expected.toJSON(), scroogePerson.toJSON());
  }

  @Test
  public void testConvertOptionalPrimitiveMap() throws Exception{
    ThriftType.StructType scroogeMap = new ScroogeStructConverter().convert(TestOptionalMap.class);
    ThriftType.StructType expected = new ThriftSchemaConverter().toStructType(org.apache.parquet.thrift.test.TestOptionalMap.class);
    assertEquals(expected,scroogeMap);
  }
}
