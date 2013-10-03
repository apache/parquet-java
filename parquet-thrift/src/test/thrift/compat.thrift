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

struct RenameStructV1 {
  1: required string nameChanged
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