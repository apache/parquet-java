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
  1: string street,
  2: required string zip
}

struct Phone {
  1: string mobile
  2: string work
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
  3: Address address,
  4: string info,
  5: required Phone phone
}

struct TestPersonWithAllInformation {
   1: required Name name,
   2: optional i32 age,
   3: Address address,
   4: optional Address working_address,
   5: string info,
   6: required map<string,Phone> phone_map,
   7: optional map<string,Phone> opt_phone_map,
   8: optional set<string> interests,
   9: optional list<string> key_words
}

struct TestMap {
  1: required map<i16,string> short_map;
  2: required map<i32,string> int_map;
  3: required map<byte,string> byt_map;
}
