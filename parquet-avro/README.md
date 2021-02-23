<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

Apache Avro integration
======

**TODO**: Add description and examples how to use parquet-avro

## Available options via Hadoop Configuration

### Configuration for reading

| Name                                    | Type      | Description                                                          |
|-----------------------------------------|-----------|----------------------------------------------------------------------|
| `parquet.avro.data.supplier`            | `Class`   | The implementation of the interface org.apache.parquet.avro.AvroDataSupplier. Available implementations in the library: GenericDataSupplier, ReflectDataSupplier, SpecificDataSupplier.<br/>The default value is `org.apache.parquet.avro.SpecificDataSupplier` |
| `parquet.avro.read.schema`              | `String`  | The Avro schema to be used for reading. It shall be compatible with the file schema. The file schema will be used directly if not set. |
| `parquet.avro.projection`               | `String`  | The Avro schema to be used for projection.                           |
| `parquet.avro.compatible`               | `boolean` | Flag for compatibility mode. `true` for materializing Avro `IndexedRecord` objects, `false` for materializing the related objects for either generic, specific, or reflect records.<br/>The default value is `true`. |

### Configuration for writing

| Name                                    | Type      | Description                                                          |
|-----------------------------------------|-----------|----------------------------------------------------------------------|
| `parquet.avro.write.data.supplier`      | `Class`   | The implementation of the interface org.apache.parquet.avro.AvroDataSupplier. Available implementations in the library: GenericDataSupplier, ReflectDataSupplier, SpecificDataSupplier.<br/>The default value is `org.apache.parquet.avro.SpecificDataSupplier` |
| `parquet.avro.schema`                   | `String`  | The Avro schema to be used for generating the Parquet schema of the file. |
| `parquet.avro.write-old-list-structure` | `boolean` | Flag whether to write list structures in the old way (2 levels) or the new one (3 levels). When writing at 2 levels no null values are available at the element level.<br/>The default value is `true` |
| `parquet.avro.add-list-element-records` | `boolean` | Flag whether to assume that any repeated element in the schema is a list element.<br/>The default value is `true`. |
| `parquet.avro.write-parquet-uuid`       | `boolean` | Flag whether to write the [Parquet UUID logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#uuid) in case of an [Avro UUID type](https://avro.apache.org/docs/current/spec.html#UUID) is present.<br/>The default value is `false`. |
