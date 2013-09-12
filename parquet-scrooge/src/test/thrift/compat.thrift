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