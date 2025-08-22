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
