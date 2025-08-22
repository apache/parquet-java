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

# Parquet Java Examples

This directory contains real examples demonstrating how to use the Apache Parquet Java library.

## Examples Overview

### 1. BasicReadWriteExample.java
Demonstrates basic reading and writing of Parquet files using the example API.

- Schema definition
- Writing data with compression
- Reading data and calculating statistics
- Basic configuration options

**Run with:**
```bash
mvn exec:java -Dexec.mainClass="org.apache.parquet.examples.BasicReadWriteExample"
```

### 2. AvroIntegrationExample.java
Shows how to integrate Avro with Parquet format.

- Avro schema definition
- Writing Avro records to Parquet
- Reading Parquet files as Avro records
- Schema projection for performance

**Run with:**
```bash
mvn exec:java -Dexec.mainClass="org.apache.parquet.examples.AvroIntegrationExample"
```

### 3. AdvancedFeaturesExample.java
Demonstrates advanced Parquet features.

- Predicate pushdown filtering
- Performance optimization with projections
- Complex filter conditions

**Run with:**
```bash
mvn exec:java -Dexec.mainClass="org.apache.parquet.examples.AdvancedFeaturesExample"
```

## Prerequisites

- Java 8 or higher
- Maven 3.6+

## Contributing

Feel free to contribute additional examples by:

1. Creating new example classes
2. Improving existing examples
3. Adding more comprehensive test cases
4. Updating documentation
