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
| `parquet.avro.readInt96AsFixed`        | `boolean` | Flag for handling the `INT96` Parquet types. `true` for converting it to the `fixed` Avro type, `false` for not handling `INT96` types (throwing exception).<br/>The default value is `false`.<br/>**NOTE: The `INT96` Parquet type is deprecated. This option is only to support old data.** |

### Configuration for writing

| Name                                    | Type      | Description                                                          |
|-----------------------------------------|-----------|----------------------------------------------------------------------|
| `parquet.avro.write.data.supplier`      | `Class`   | The implementation of the interface org.apache.parquet.avro.AvroDataSupplier. Available implementations in the library: GenericDataSupplier, ReflectDataSupplier, SpecificDataSupplier.<br/>The default value is `org.apache.parquet.avro.SpecificDataSupplier` |
| `parquet.avro.schema`                   | `String`  | The Avro schema to be used for generating the Parquet schema of the file. |
| `parquet.avro.write-old-list-structure` | `boolean` | Flag whether to write list structures in the old way (2 levels) or the new one (3 levels). When writing at 2 levels no null values are available at the element level.<br/>The default value is `true` |
| `parquet.avro.add-list-element-records` | `boolean` | Flag whether to assume that any repeated element in the schema is a list element.<br/>The default value is `true`. |
| `parquet.avro.write-parquet-uuid`       | `boolean` | Flag whether to write the [Parquet UUID logical type](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#uuid) in case of an [Avro UUID type](https://avro.apache.org/docs/current/spec.html#UUID) is present.<br/>The default value is `false`. |
| `parquet.avro.writeFixedAsInt96`    | `String` | Comma separated list of paths pointing to Avro schema elements which are to be converted to `INT96` Parquet types.<br/>The path is a `'.'` separated list of field names and does not contain the name of the schema nor the namespace. The type of the referenced schema elements must be `fixed` with the size of 12 bytes.<br/>**NOTE: The `INT96` Parquet type is deprecated. This option is only to support old data.** |

## Writing Data

**To write data using Parquet-Avro, follow these steps**:

1. **Create an Avro schema**: Define the structure of your data using an Avro schema file (.avsc).
2. **Create a Parquet writer**: Use the AvroParquetWriter class to create a writer with the Avro schema.
3. **Write records**: Create Avro records and write them to the Parquet file using the writer.

**Example**

// Create an Avro schema
Schema schema = new Schema.Parser().parse(new File("user.avsc"));

// Create a Parquet writer
ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new File("data.parquet"))
    .withSchema(schema)
    .withConf(new Configuration())
    .withCompressionCodec(CompressionCodecName.SNAPPY)
    .build();

// Create an Avro record
GenericRecord record = new GenericData.Record(schema);
record.put("id", 1);
record.put("name", "John Doe");
record.put("email", "john.doe@example.com");

// Write the record to Parquet
writer.write(record);
writer.close();


**Reading Data**

To read data using Parquet-Avro, follow these steps:

1. **Create a Parquet reader**: Use the AvroParquetReader class to create a reader with the Avro schema.
2. **Read records**: Read Avro records from the Parquet file using the reader.

**Example**

// Create a Parquet reader
ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new File("data.parquet"))
    .withSchema(schema)
    .build();

// Read a record from Parquet
GenericRecord record = reader.read();

// Print the record
System.out.println(record.get("id"));
System.out.println(record.get("name"));
System.out.println(record.get("email"));

// Close the reader
reader.close();

## major aspects of Apache Parquet-Avro:

**Writing Data**

**Python**

import pyarrow.parquet as pq
import avro.schema
from avro.datafile import DataFileWriter
from (link unavailable) import DatumWriter

# Create an Avro schema
schema = avro.schema.Parse(json.dumps({
    "type": "record",
    "name": "example",
    "fields": [
        {"name": "numbers", "type": "array", "items": "int"}
    ]
}))

# Create an Avro data file
with open("example.avro", "wb") as f:
    writer = DataFileWriter(f, DatumWriter(), schema)
    writer.append({"numbers": [1, 2, 3, 4, 5]})
    writer.close()

# Create a Parquet file from the Avro data file
table = pq.read_table("example.avro")
pq.write_table(table, "example.parquet")


**C++**

#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <avro/Writer.hh>
#include <avro/Reader.hh>

int main() {
    // Create an Avro schema
    avro::ValidSchema schema = avro::parseJsonSchema(
        "{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"numbers\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}]}");

    // Create an Avro data file
    std::ofstream f("example.avro", std::ios::binary);
    avro::DataFileWriter<avro::OutputStream> writer(f, schema);
    writer.write({"numbers", {1, 2, 3, 4, 5}});
    writer.close();

    // Create a Parquet file from the Avro data file
    parquet::arrow::WriterProperties writer_properties;
    parquet::arrow::WriteTable(table, "example.parquet", writer_properties);
}


**Reading Data**

**Python**

import pyarrow.parquet as pq
import avro.schema
from avro.datafile import DataFileReader

# Read the Avro data file
with open("example.avro", "rb") as f:
    reader = DataFileReader(f)
    for record in reader:
        print(record)

# Read the Parquet file
table = pq.read_table("example.parquet")
print(table)


**C++**

int main() {
    // Read the Avro data file
    std::ifstream f("example.avro", std::ios::binary);
    avro::DataFileReader<avro::InputStream> reader(f);
    avro::GenericDatum datum;
    while (reader.read(datum)) {
        std::cout << datum << std::endl;
    }

    // Read the Parquet file
    parquet::arrow::ReaderProperties reader_properties;
    std::shared_ptr<arrow::Table> table = parquet::arrow::OpenFile("example.parquet", reader_properties);
    std::cout << table->ToString() << std::endl;
}


**Schema Evolution**

**Python**

import avro.schema

# Create an initial Avro schema
initial_schema = avro.schema.Parse(json.dumps({
    "type": "record",
    "name": "example",
    "fields": [
        {"name": "numbers", "type": "array", "items": "int"}
    ]
}))

# Create a new Avro schema with an added field
new_schema = avro.schema.Parse(json.dumps({
    "type": "record",
    "name": "example",
    "fields": [
        {"name": "numbers", "type": "array", "items": "int"},
        {"name": "strings", "type": "array", "items": "string"}
    ]
}))

# Check if the new schema is compatible with the initial schema
if avro.schema.can_read(initial_schema, new_schema):
    print("Schemas are compatible")
else:
    print("Schemas are not compatible")


**C++**

int main() {
    // Create an initial Avro schema
    avro::ValidSchema initial_schema = avro::parseJsonSchema(
        "{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"numbers\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}]}");

    // Create a new Avro schema with an added field
    avro::ValidSchema new_schema = avro::parseJsonSchema(
        "{\"type\": \"record\", \"name\": \"example\", \"fields\": [{\"name\": \"numbers\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}, {\"name\": \"








