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
namespace java parquet.thrift.test
struct TestListsInMap {
  1: string name,
  2: map<list<string>,list<string>> names,
}

struct Name {
  1: required string first_name,
  2: optional string last_name
}

struct Address {
  1: required string street,
  2: required string zip
}

struct AddressWithStreetWithDefaultRequirement {
  1: string street,
  2: required string zip
}

struct Phone {
  1: required string mobile
  2: required string work
}

struct TestPerson {
  1: required Name name,
  2: optional i32 age,
  3: Address address,
  4: string info
}


struct RequiredMapFixture {
  1: optional string name,
  2: required map<string,string> mavalue
}

struct RequiredListFixture {
  1: optional string info,
  2: required list<Name> names
}

struct RequiredSetFixture {
  1: optional string info,
  2: required set<Name> names
}

struct RequiredPrimitiveFixture {
  1: required bool test_bool,
  2: required byte test_byte,
  3: required i16 test_i16,
  4: required i32 test_i32,
  5: required i64 test_i64,
  6: required double test_double,
  7: required string test_string,
  8: optional string info_string
}



struct TestPersonWithRequiredPhone {
  1: required Name name,
  2: optional i32 age,
  3: required Address address,
  4: optional string info,
  5: required Phone phone
}

struct TestPersonWithAllInformation {
   1: required Name name,
   2: optional i32 age,
   3: required Address address,
   4: optional Address working_address,
   5: optional string info,
   6: required map<string,Phone> phone_map,
   7: optional set<string> interests,
   8: optional list<string> key_words
}

struct TestMapComplex{
  1: required map<Phone,Address> phone_address_map
}

struct TestMapPrimitiveKey {
  1: required map<i16,string> short_map,
  2: required map<i32,string> int_map,
  3: required map<byte,string> byt_map,
  4: required map<bool,string> bool_map,
  5: required map<i64,string> long_map,
  6: required map<double,string> double_map,
  7: required map<string,string> string_map;
}

struct TestOptionalMap {
   1: optional map<i16,string> short_map,
   2: optional map<i32,string> int_map,
   3: optional map<byte,string> byt_map,
   4: optional map<bool,string> bool_map,
   5: optional map<i64,string> long_map,
   6: optional map<double,string> double_map,
   7: optional map<string,string> string_map
}

struct TestListPrimitive {
  1: required list<i16> short_list,
  2: required list<i32> int_list,
  3: required list<i64> long_list,
  4: required list<byte> byte_list,
  5: required list<string> string_list,
  6: required list<bool> bool_list,
  7: required list<double> double_list,
}

struct TestSetPrimitive {
  1: required set<i16> short_list,
  2: required set<i32> int_list,
  3: required set<i64> long_list,
  4: required set<byte> byte_list,
  5: required set<string> string_list,
  6: required set<bool> bool_list,
  7: required set<double> double_list
}

struct TestMapPrimitiveValue {
  1: required map<string,i16> short_map,
  2: required map<string,i32> int_map,
  3: required map<string,byte> byt_map,
  4: required map<string,bool> bool_map,
  5: required map<string,i64> long_map,
  6: required map<string,double> double_map,
  7: required map<string,string> string_map
}

union TestUnion {
  1: TestPerson first_person
  2: TestMapComplex second_map
}

enum Operation {
  ADD = 1,
  SUBTRACT = 2,
  MULTIPLY = 3,
  DIVIDE = 4
}

struct TestFieldOfEnum{
 1: required Operation op
 2: optional Operation op2
}

