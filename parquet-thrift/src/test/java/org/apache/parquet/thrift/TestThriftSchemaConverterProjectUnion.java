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

import static org.apache.parquet.thrift.TestThriftSchemaConverter.shouldGetProjectedSchema;

import org.apache.parquet.thrift.test.compat.ListOfUnions;
import org.apache.parquet.thrift.test.compat.MapWithUnionKey;
import org.apache.parquet.thrift.test.compat.MapWithUnionValue;
import org.apache.parquet.thrift.test.compat.NestedNestedUnion;
import org.apache.parquet.thrift.test.compat.NestedUnion;
import org.apache.parquet.thrift.test.compat.OptionalInsideRequired;
import org.apache.parquet.thrift.test.compat.RequiredInsideOptional;
import org.apache.parquet.thrift.test.compat.StructWithNestedUnion;
import org.apache.parquet.thrift.test.compat.StructWithOptionalUnionOfStructs;
import org.apache.parquet.thrift.test.compat.UnionOfStructs;
import org.apache.parquet.thrift.test.compat.UnionV2;
import org.junit.Test;

public class TestThriftSchemaConverterProjectUnion {

  /**
   * Test projecting into a top level union
   */
  @Test
  public void testTopLevelUnions() {

    // very simple case, a union of structs where each struct has only 1 field
    shouldGetProjectedSchema(
        "aLong/**",
        "aLong",
        "message ParquetSchema {\n" + "  optional group aString = 1 {\n"
            + "    required binary s (UTF8) = 1;\n"
            + "  }\n"
            + "  optional group aLong = 2 {\n"
            + "    required int64 l = 1;\n"
            + "  }\n"
            + "  optional group aNewBool = 3 {\n"
            + "    required boolean b = 1;\n"
            + "  }\n"
            + "}",
        UnionV2.class);

    // a union of structs, where each struct has more than one field
    // we should still only get one field per child here though
    shouldGetProjectedSchema(
        "aNewBool/**",
        "aNewBool",
        "message ParquetSchema {\n" + "  optional group structV3 = 1 {\n"
            + "    required binary name (UTF8) = 1;\n"
            + "  }\n"
            + "  optional group structV4 = 2 {\n"
            + "    required binary name (UTF8) = 1;\n"
            + "  }\n"
            + "  optional group aNewBool = 3 {\n"
            + "    required boolean b = 1;\n"
            + "  }\n"
            + "}",
        UnionOfStructs.class);
  }

  /**
   * An optional union should be dropped if not selected.
   */
  @Test
  public void optionalUnionShouldBeDropped() {
    shouldGetProjectedSchema(
        "name",
        "name",
        "message ParquetSchema {\n" + "  required binary name (UTF8) = 1;\n" + "}",
        StructWithOptionalUnionOfStructs.class);
  }

  /**
   * An optional union inside a required struct should still be
   * dropped if not selected.
   */
  @Test
  public void optionalUnionInRequiredStructShouldBeDropped() {
    shouldGetProjectedSchema(
        "name",
        "name",
        "message ParquetSchema {\n" + "  required binary name (UTF8) = 1;\n" + "}",
        OptionalInsideRequired.class);
  }

  /**
   * A required union inside of an un-selected optional struct
   * should get dropped along with the optional struct.
   */
  @Test
  public void requiredUnionInsideOptionalStructShouldBeDropped() {
    // test a required union inside an optional struct
    shouldGetProjectedSchema(
        "name",
        "name",
        "message ParquetSchema {\n" + "  required binary name (UTF8) = 1;\n" + "}",
        RequiredInsideOptional.class);
  }

  /**
   * A required union inside a selected optional struct should keep
   * one sentinel column for each "kind" of union member.
   */
  @Test
  public void requiredUnionInsideOptionalStructShouldBeKeptIfParentSelected() {
    shouldGetProjectedSchema(
        "aStruct/name",
        "aStruct.name",
        "message ParquetSchema {\n" + "  optional group aStruct = 2 {\n"
            + "    required binary name (UTF8) = 1;\n"
            + "    required group aUnion = 2 {\n"
            + "      optional group structV3 = 1 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group structV4 = 2 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group aNewBool = 3 {\n"
            + "        required boolean b = 1;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}",
        RequiredInsideOptional.class);
  }

  /**
   * Selecting only one "kind" of a union should trigger keeping one sentinel
   * column from the rest of the "kinds" of the union.
   */
  @Test
  public void selectingOneUnionMemberKeepsSentinels() {
    shouldGetProjectedSchema(
        "aUnion/structV4/addedStruct/gender",
        "aUnion.structV4.addedStruct.gender",
        "message ParquetSchema {\n" + "  optional group aUnion = 2 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group structV4 = 2 {\n"
            + "      optional group addedStruct = 4 {\n"
            + "        optional binary gender (UTF8) = 3;\n"
            + "      }\n"
            + "    }\n"
            + "    optional group aNewBool = 3 {\n"
            + "      required boolean b = 1;\n"
            + "    }\n"
            + "  }\n"
            + "}",
        StructWithOptionalUnionOfStructs.class);
  }

  /**
   * In the case of a union inside a union (and union inside union inside union),
   * even though the fields will be "optional" because of how unions
   * are stored, we still need to grab sentinel columns from
   * the members of the union.
   */
  @Test
  public void testUnionInsideUnion() {

    shouldGetProjectedSchema(
        "structV3/age",
        "structV3.age",
        "message ParquetSchema {\n" + "  optional group structV3 = 1 {\n"
            + "    optional binary age (UTF8) = 2;\n"
            + "  }\n"
            + "  optional group unionOfStructs = 2 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group structV4 = 2 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group aNewBool = 3 {\n"
            + "      required boolean b = 1;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group aLong = 3 {\n"
            + "    required int64 l = 1;\n"
            + "  }\n"
            + "}",
        NestedUnion.class);

    shouldGetProjectedSchema(
        "unionOfStructs/structV4/addedStruct/gender",
        "unionOfStructs.structV4.addedStruct.gender",
        "message ParquetSchema {\n" + "  optional group structV3 = 1 {\n"
            + "    required binary name (UTF8) = 1;\n"
            + "  }\n"
            + "  optional group unionOfStructs = 2 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group structV4 = 2 {\n"
            + "      optional group addedStruct = 4 {\n"
            + "        optional binary gender (UTF8) = 3;\n"
            + "      }\n"
            + "    }\n"
            + "    optional group aNewBool = 3 {\n"
            + "      required boolean b = 1;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group aLong = 3 {\n"
            + "    required int64 l = 1;\n"
            + "  }\n"
            + "}\n",
        NestedUnion.class);

    shouldGetProjectedSchema(
        "unionV2/aLong/**",
        "unionV2.aLong",
        "message ParquetSchema {\n" + "  optional group nestedUnion = 1 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group unionOfStructs = 2 {\n"
            + "      optional group structV3 = 1 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group structV4 = 2 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group aNewBool = 3 {\n"
            + "        required boolean b = 1;\n"
            + "      }\n"
            + "    }\n"
            + "    optional group aLong = 3 {\n"
            + "      required int64 l = 1;\n"
            + "    }\n"
            + "  }\n"
            + "  optional group unionV2 = 2 {\n"
            + "    optional group aString = 1 {\n"
            + "      required binary s (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group aLong = 2 {\n"
            + "      required int64 l = 1;\n"
            + "    }\n"
            + "    optional group aNewBool = 3 {\n"
            + "      required boolean b = 1;\n"
            + "    }\n"
            + "  }\n"
            + "}",
        NestedNestedUnion.class);
  }

  @Test
  public void testListOfUnions() {

    // selecting a field from an optional list of unions should also choose a sentinel field
    // for the rest of the union members
    // at the same time, this should also drop the required list of unions, because it is safe to
    // drop even a required but repeated group
    shouldGetProjectedSchema(
        "optListUnion/structV3/age",
        "optListUnion.structV3.age",
        "message ParquetSchema {\n" + "  optional group optListUnion (LIST) = 1 {\n"
            + "    repeated group optListUnion_tuple {\n"
            + "      optional group structV3 = 1 {\n"
            + "        optional binary age (UTF8) = 2;\n"
            + "      }\n"
            + "      optional group structV4 = 2 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group aNewBool = 3 {\n"
            + "        required boolean b = 1;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}",
        ListOfUnions.class);

    // same goes for selecting a field from a required list of unions
    // and at the same time, the optional list of unions should be dropped too
    shouldGetProjectedSchema(
        "reqListUnion/structV3/age",
        "reqListUnion.structV3.age",
        "message ParquetSchema {\n" + "  required group reqListUnion (LIST) = 2 {\n"
            + "    repeated group reqListUnion_tuple {\n"
            + "      optional group structV3 = 1 {\n"
            + "        optional binary age (UTF8) = 2;\n"
            + "      }\n"
            + "      optional group structV4 = 2 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group aNewBool = 3 {\n"
            + "        required boolean b = 1;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}",
        ListOfUnions.class);
  }

  @Test
  public void testMapWithUnionKey() {
    shouldGetProjectedSchema(
        "optMapWithUnionKey/key/**",
        "optMapWithUnionKey.key",
        "message ParquetSchema {\n" + "  optional group optMapWithUnionKey (MAP) = 1 {\n"
            + "    repeated group key_value {\n"
            + "      required group key {\n"
            + "        optional group structV3 = 1 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "          optional binary age (UTF8) = 2;\n"
            + "          optional binary gender (UTF8) = 3;\n"
            + "        }\n"
            + "        optional group structV4 = 2 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "          optional binary age (UTF8) = 2;\n"
            + "          optional binary gender (UTF8) = 3;\n"
            + "          optional group addedStruct = 4 {\n"
            + "            required binary name (UTF8) = 1;\n"
            + "            optional binary age (UTF8) = 2;\n"
            + "            optional binary gender (UTF8) = 3;\n"
            + "          }\n"
            + "        }\n"
            + "        optional group aNewBool = 3 {\n"
            + "          required boolean b = 1;\n"
            + "        }\n"
            + "      }\n"
            + "      optional group value {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      } "
            + "    }\n"
            + "  }\n"
            + "}",
        MapWithUnionKey.class);

    shouldGetProjectedSchema(
        "optMapWithUnionKey/key/**;optMapWithUnionKey/value/gender",
        "optMapWithUnionKey.{key,value.gender}",
        "message ParquetSchema {\n" + "  optional group optMapWithUnionKey (MAP) = 1 {\n"
            + "    repeated group key_value {\n"
            + "      required group key {\n"
            + "        optional group structV3 = 1 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "          optional binary age (UTF8) = 2;\n"
            + "          optional binary gender (UTF8) = 3;\n"
            + "        }\n"
            + "        optional group structV4 = 2 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "          optional binary age (UTF8) = 2;\n"
            + "          optional binary gender (UTF8) = 3;\n"
            + "          optional group addedStruct = 4 {\n"
            + "            required binary name (UTF8) = 1;\n"
            + "            optional binary age (UTF8) = 2;\n"
            + "            optional binary gender (UTF8) = 3;\n"
            + "          }\n"
            + "        }\n"
            + "        optional group aNewBool = 3 {\n"
            + "          required boolean b = 1;\n"
            + "        }\n"
            + "      }\n"
            + "      optional group value {\n"
            + "        optional binary gender (UTF8) = 3;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}",
        MapWithUnionKey.class);
  }

  @Test
  public void testMapWithUnionValue() {
    shouldGetProjectedSchema(
        "optMapWithUnionValue/key/**;optMapWithUnionValue/value/structV4/addedStruct/gender",
        "optMapWithUnionValue.{key,value.structV4.addedStruct.gender}",
        "message ParquetSchema {\n" + "  optional group optMapWithUnionValue (MAP) = 1 {\n"
            + "    repeated group key_value {\n"
            + "      required group key {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "        optional binary age (UTF8) = 2;\n"
            + "        optional binary gender (UTF8) = 3;\n"
            + "      }\n"
            + "      optional group value {\n"
            + "        optional group structV3 = 1 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "        }\n"
            + "        optional group structV4 = 2 {\n"
            + "          optional group addedStruct = 4 {\n"
            + "            optional binary gender (UTF8) = 3;\n"
            + "          }\n"
            + "        }\n"
            + "        optional group aNewBool = 3 {\n"
            + "          required boolean b = 1;\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}",
        MapWithUnionValue.class);
  }

  /**
   * Tests a complicated struct that contains required, optional, and unspecified
   * members of a few different corner cases.
   */
  @Test
  public void testMessyNestedUnions() {
    shouldGetProjectedSchema(
        "reqStructWithUnionV2/name",
        "reqStructWithUnionV2.name",
        "message ParquetSchema {\n" + "  required group reqUnionOfStructs = 2 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group structV4 = 2 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group aNewBool = 3 {\n"
            + "      required boolean b = 1;\n"
            + "    }\n"
            + "  }\n"
            + "  required group reqNestedUnion = 5 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group unionOfStructs = 2 {\n"
            + "      optional group structV3 = 1 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group structV4 = 2 {\n"
            + "        required binary name (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group aNewBool = 3 {\n"
            + "        required boolean b = 1;\n"
            + "      }\n"
            + "    }\n"
            + "    optional group aLong = 3 {\n"
            + "      required int64 l = 1;\n"
            + "    }\n"
            + "  }\n"
            + "  required group reqStructWithUnionV2 = 8 {\n"
            + "    required binary name (UTF8) = 1;\n"
            + "    required group aUnion = 2 {\n"
            + "      optional group aString = 1 {\n"
            + "        required binary s (UTF8) = 1;\n"
            + "      }\n"
            + "      optional group aLong = 2 {\n"
            + "        required int64 l = 1;\n"
            + "      }\n"
            + "      optional group aNewBool = 3 {\n"
            + "        required boolean b = 1;\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  required group reqUnionStructUnion = 11 {\n"
            + "    optional group structV3 = 1 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "    }\n"
            + "    optional group structWithUnionOfStructs = 2 {\n"
            + "      required binary name (UTF8) = 1;\n"
            + "      required group aUnion = 2 {\n"
            + "        optional group structV3 = 1 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "        }\n"
            + "        optional group structV4 = 2 {\n"
            + "          required binary name (UTF8) = 1;\n"
            + "        }\n"
            + "        optional group aNewBool = 3 {\n"
            + "          required boolean b = 1;\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "    optional group aLong = 3 {\n"
            + "      required int64 l = 1;\n"
            + "    }\n"
            + "  }\n"
            + "}",
        StructWithNestedUnion.class);
  }
}
