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


struct StructWithReorderedOptionalFields {
  3: optional i32 fieldThree,
  2: optional i32 fieldTwo,
  1: optional i32 fieldOne,
}

struct TestPersonWithRequiredPhone {
  1: required Name name,
  2: optional i32 age,
  3: Address address,
  4: string info,
  5: required Phone phone
}

struct StructWithIndexStartsFrom4 {
  6: required Phone phone
}

struct StructWithExtraField {
  3: required Phone extraPhone,
  6: required Phone phone
}

struct EmptyStruct {
}

union UnionWithEmptyStruct {
  1: EmptyStruct empty
  2: Name name
}

struct FormerlyEmptyStruct {
  1: optional i32 newField
}

union UnionWithFormerlyEmptyStruct {
  1: FormerlyEmptyStruct empty
  2: Name name
}
