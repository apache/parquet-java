namespace java parquet.thrift.test
struct TestListsInMap {
  1: string name,
  2: map<list<string>,list<string>> names,
}

struct Name {
  1: required string first_name,
  2: string last_name
}

struct Address {
  1: string street,
  2: required string zip
}

struct TestPerson {
  1: required Name name,
  2: optional i32 age,
  3: Address address

}

