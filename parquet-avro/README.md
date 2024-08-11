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



## Major aspects of Apache Parquet-Avro:

**Parquet aspects**:

1. **Columnar storage**: Stores data in columns instead of rows, reducing storage and improving query performance.
2. **Efficient compression**: Supports various compression algorithms, reducing storage and improving data transfer.
3. **Schema evolution**: Allows for schema changes without rewriting existing data.
4. **High-performance querying**: Optimized for fast querying and data retrieval.

**Avro aspects**:

1. **Data serialization**: Provides a compact, fast, and efficient way to serialize and deserialize data.
2. **Schema-based**: Uses a schema to define data structures, ensuring data consistency and compatibility.
3. **Language-independent**: Supports multiple programming languages, including Java, Python, and C++.
4. **Rich data structures**: Supports complex data structures, including records, enums, and arrays.

## Parquet-Avro integration:

1. **Avro schema integration**: Uses Avro schemas to define Parquet data structures.
2. **Efficient Avro data storage**: Stores Avro data in Parquet files, leveraging Parquet's columnar storage and compression.
3. **Seamless data exchange**: Enables easy data exchange between Avro and Parquet formats.

By combining Parquet's columnar storage and Avro's data serialization, Parquet-Avro provides a powerful and efficient way to store and query large datasets.

## Specific  and practical applications of Apache Parquet-Avro

1. **Data ingestion and processing**: Using Parquet-Avro to efficiently store and process large amounts of data from various sources, like logs, sensors, or social media.

2. **Data transformation and aggregation**: Leveraging Parquet-Avro's columnar storage to perform fast data transformations and aggregations, like data warehousing or business intelligence workloads.

3. **Real-time data analytics**: Utilizing Parquet-Avro's efficient storage and querying capabilities to power real-time data analytics, like fraud detection or live dashboards.

4. **Machine learning data preparation**: Using Parquet-Avro to store and prepare large datasets for machine learning model training, feature engineering, and data augmentation.

5. **Data archiving and compliance**: Employing Parquet-Avro's compression and encryption features to archive large datasets for long-term storage and compliance purposes.

6. **Cloud data migration**: Migrating large datasets to cloud storage services, like Amazon S3 or Google Cloud Storage, using Parquet-Avro for efficient data transfer and storage.

7. **Data integration and ETL**: Using Parquet-Avro as a common format for data integration and ETL (Extract, Transform, Load) processes, ensuring data consistency and compatibility.

8. **IoT data processing and analytics**: Storing and processing large amounts of IoT sensor data using Parquet-Avro, enabling efficient data analytics and insights.


## Writing Avro Data to Parquet Files

- Use `AvroParquetWriter` to write Avro data to Parquet files.
- Set the Avro schema using `writer.setSchema(AvroSchema)`.

## Reading Avro Data from Parquet Files

- Use `AvroParquetReader` to read Avro data from Parquet files.
- Get the Avro schema using `reader.getSchema()`.

## Converting Avro Schemas to Parquet Schemas

- Use `AvroSchemaConverter` to convert Avro schemas to Parquet schemas.
- Convert an Avro schema to a Parquet schema using `converter.convert(AvroSchema)`.

## Additional API Classes and Methods

- `org.apache.parquet.hadoop.ParquetWriter`: Writes Parquet files.
- `org.apache.parquet.hadoop.ParquetReader`: Reads Parquet files.
- `org.apache.avro.Schema`: Represents an Avro schema.
- `org.apache.parquet.schema.MessageType`: Represents a Parquet schema.

