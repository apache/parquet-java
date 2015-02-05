/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace java parquet.thrift.test.compat
struct StructV1 {
  1: required string name
}
struct StructV2 {
  1: required string name,
  2: optional string age
}
struct StructV3 {
  1: required string name,
  2: optional string age,
  3: optional string gender
}

struct StructV4WithExtracStructField {
  1: required string name,
  2: optional string age,
  3: optional string gender,
  4: optional StructV3 addedStruct
}

struct RenameStructV1 {
  1: required string nameChanged
}

enum NumberEnum {
  ONE = 1,
  TWO = 2,
  THREE = 3
}

enum NumberEnumWithMoreValue {
  ONE = 1,
  TWO = 2,
  THREE = 3,
  FOUR = 4
}

struct StructWithEnum {
 1: required NumberEnum num
}

struct StructWithMoreEnum {
 1: required NumberEnumWithMoreValue num
}

struct TypeChangeStructV1{
  1: required i16 name
}

struct OptionalStructV1{
  1: optional string name
}

struct DefaultStructV1{
  1: string name
}

struct AddRequiredStructV1{
  1: required string name,
  2: required string anotherName
}

struct MapStructV1{
  1: required map<StructV1,string> map1
}

struct MapValueStructV1{
  1: required map<string,StructV1> map1
}

struct MapStructV2{
  1: required map<StructV2,string> map1
}

struct MapValueStructV2{
  1: required map<string,StructV2> map1
}

struct MapAddRequiredStructV1{
  1: required map<AddRequiredStructV1,string> map1
}

struct SetStructV1{
  1: required set<StructV1> set1
}

struct SetStructV2{
  1: required set<StructV2> set1
}

struct ListStructV1{
  1: required list<StructV1> list1
}

struct ListStructV2{
  1: required list<StructV2> list1
}
