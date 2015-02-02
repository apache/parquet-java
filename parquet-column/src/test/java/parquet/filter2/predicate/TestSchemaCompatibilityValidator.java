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
package parquet.filter2.predicate;

import org.junit.Test;

import parquet.filter2.predicate.Operators.BinaryColumn;
import parquet.filter2.predicate.Operators.IntColumn;
import parquet.filter2.predicate.Operators.LongColumn;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static parquet.filter2.predicate.FilterApi.and;
import static parquet.filter2.predicate.FilterApi.binaryColumn;
import static parquet.filter2.predicate.FilterApi.eq;
import static parquet.filter2.predicate.FilterApi.gt;
import static parquet.filter2.predicate.FilterApi.intColumn;
import static parquet.filter2.predicate.FilterApi.longColumn;
import static parquet.filter2.predicate.FilterApi.ltEq;
import static parquet.filter2.predicate.FilterApi.not;
import static parquet.filter2.predicate.FilterApi.notEq;
import static parquet.filter2.predicate.FilterApi.or;
import static parquet.filter2.predicate.FilterApi.userDefined;
import static parquet.filter2.predicate.SchemaCompatibilityValidator.validate;

public class TestSchemaCompatibilityValidator {
  private static final BinaryColumn stringC = binaryColumn("c");
  private static final LongColumn longBar = longColumn("x.bar");
  private static final IntColumn intBar = intColumn("x.bar");
  private static final LongColumn lotsOfLongs = longColumn("lotsOfLongs");

  private static final String schemaString =
      "message Document {\n"
          + "  required int32 a;\n"
          + "  required binary b;\n"
          + "  required binary c (UTF8);\n"
          + "  required group x { required int32 bar; }\n"
          + "  repeated int64 lotsOfLongs;\n"
          + "}\n";

  private static final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

  private static final FilterPredicate complexValid =
      and(
          or(ltEq(stringC, Binary.fromString("foo")),
              and(
                  not(or(eq(intBar, 17), notEq(intBar, 17))),
                  userDefined(intBar, DummyUdp.class))),
          or(gt(stringC, Binary.fromString("bar")), notEq(stringC, Binary.fromString("baz"))));

  static class LongDummyUdp extends UserDefinedPredicate<Long> {
    @Override
    public boolean keep(Long value) {
      return false;
    }

    @Override
    public boolean canDrop(Statistics<Long> statistics) {
      return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Long> statistics) {
      return false;
    }
  }

  private static final FilterPredicate complexWrongType =
      and(
          or(ltEq(stringC, Binary.fromString("foo")),
              and(
                  not(or(eq(longBar, 17L), notEq(longBar, 17L))),
                  userDefined(longBar, LongDummyUdp.class))),
          or(gt(stringC, Binary.fromString("bar")), notEq(stringC, Binary.fromString("baz"))));

  private static final FilterPredicate complexMixedType =
      and(
          or(ltEq(stringC, Binary.fromString("foo")),
              and(
                  not(or(eq(intBar, 17), notEq(longBar, 17L))),
                  userDefined(longBar, LongDummyUdp.class))),
          or(gt(stringC, Binary.fromString("bar")), notEq(stringC, Binary.fromString("baz"))));

  @Test
  public void testValidType() {
    validate(complexValid, schema);
  }

  @Test
  public void testFindsInvalidTypes() {
    try {
      validate(complexWrongType, schema);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("FilterPredicate column: x.bar's declared type (java.lang.Long) does not match the schema found in file metadata. "
          + "Column x.bar is of type: FullTypeDescriptor(PrimitiveType: INT32, OriginalType: null)\n"
          + "Valid types for this column are: [class java.lang.Integer]", e.getMessage());
    }
  }

  @Test
  public void testTwiceDeclaredColumn() {
    validate(eq(stringC, Binary.fromString("larry")), schema);

    try {
      validate(complexMixedType, schema);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("Column: x.bar was provided with different types in the same predicate. Found both: (class java.lang.Integer, class java.lang.Long)", e.getMessage());
    }

  }

  @Test
  public void testRepeatedNotSupported() {
    try {
      validate(eq(lotsOfLongs, 10l), schema);
      fail("this should throw");
    } catch (IllegalArgumentException e) {
      assertEquals("FilterPredicates do not currently support repeated columns. Column lotsOfLongs is repeated.", e.getMessage());
    }
  }
}
