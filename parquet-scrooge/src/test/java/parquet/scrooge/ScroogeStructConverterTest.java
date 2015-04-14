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
package parquet.scrooge;

import org.apache.thrift.TBase;
import org.junit.Test;
import parquet.schema.MessageType;
import parquet.scrooge.test.*;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test convert scrooge schema to Parquet Schema
 */
public class ScroogeStructConverterTest {

  /**
   * Convert ThriftStructs from a thrift class and a scrooge class, assert
   * they are the same
   * @param scroogeClass
   */
  private void shouldConvertConsistentlyWithThriftStructConverter(Class scroogeClass) {
    try {
      Class<? extends TBase<?, ?>> thriftClass = (Class<? extends TBase<?, ?>>)Class.forName(scroogeClass.getName().replaceFirst("parquet.scrooge.test", "parquet.thrift.test"));
      ThriftType.StructType structFromThriftSchemaConverter = new ThriftSchemaConverter().toStructType(thriftClass);
      ThriftType.StructType structFromScroogeSchemaConverter = new ScroogeStructConverter().convert(scroogeClass);

      assertEquals(toParquetSchema(structFromThriftSchemaConverter), toParquetSchema(structFromScroogeSchemaConverter));
    } catch (ClassNotFoundException e) {
      fail("ClassNotFoundException: " + e.getMessage());
    }
  }

  private MessageType toParquetSchema(ThriftType.StructType struct) {
    ThriftSchemaConverter sc = new ThriftSchemaConverter();
    return sc.convert(struct);
  }

  @Test
  public void testConvertPrimitiveMapKey() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestMapPrimitiveKey.class);
  }

  @Test
  public void testBinary() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(StringAndBinary.class);
  }

  @Test
  public void testUnion() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestUnion.class);
  }

  @Test
  public void testConvertPrimitiveMapValue() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestMapPrimitiveValue.class);
  }

  @Test
  public void testConvertPrimitiveList() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestListPrimitive.class);
  }

  @Test
  public void testConvertPrimitiveSet() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestSetPrimitive.class);
  }

  @Test
  public void testConvertEnum() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestFieldOfEnum.class);
  }

  @Test
  public void testMapComplex() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestMapComplex.class);
  }

  @Test
  public void testConvertStruct() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestPersonWithAllInformation.class);
  }

  @Test
  public void testDefaultFields() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(AddressWithStreetWithDefaultRequirement.class);
  }

  @Test
  public void testConvertOptionalPrimitiveMap() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(TestOptionalMap.class);
  }

  @Test
  public void testConvertNestedList() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(NestedList.class);
  }

  @Test
  public void testConvertListNestMap() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(ListNestMap.class);
  }

  @Test
  public void testConvertListNestEnum() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(ListNestEnum.class);
  }

  @Test
  public void testConvertMapNestList() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(MapNestList.class);
  }

  @Test
  public void testConvertMapNestMap() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(MapNestMap.class);
  }

  @Test
  public void testConvertMapNestSet() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(MapNestSet.class);
  }

  @Test
  public void testConvertListNestSet() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(ListNestSet.class);
  }

  @Test
  public void testConvertSetNestSet() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(SetNestSet.class);
  }

  @Test
  public void testConvertSetNestList() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(SetNestList.class);
  }

  @Test
  public void testConvertSetNestMap() throws Exception {
    shouldConvertConsistentlyWithThriftStructConverter(SetNestMap.class);
  }

}
