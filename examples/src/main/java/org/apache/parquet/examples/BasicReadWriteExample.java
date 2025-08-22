/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.examples;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.Random;

/**
 * Basic example demonstrating how to read and write Parquet files
 * using the example API provided by the Parquet library.
 *
 * This example shows:
 * 1. How to define a schema
 * 2. How to write data to a Parquet file
 * 3. How to read data from a Parquet file
 * 4. Basic configuration options
 */
public class BasicReadWriteExample {

    public static void main(String[] args) throws IOException {
        String outputFile = "data/employees.parquet";

        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("name")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("salary")
            .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("active")
            .named("employee");

        writeEmployeeData(outputFile, schema);

        readEmployeeData(outputFile);
    }

    private static void writeEmployeeData(String filename, MessageType schema) throws IOException {
        Path file = new Path(filename);

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(file)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withRowGroupSize(64 * 1024 * 1024)
            .withPageSize(1024 * 1024)
            .withDictionaryEncoding(true)
            .config(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString())
            .build()) {

            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Random random = new Random(42);

            for (int i = 0; i < 1000; i++) {
                Group group = factory.newGroup()
                    .append("id", i)
                    .append("name", "Employee " + i)
                    .append("salary", 50000.0 + random.nextDouble() * 50000.0)
                    .append("active", random.nextBoolean());

                writer.write(group);
            }
        }
    }

    private static void readEmployeeData(String filename) throws IOException {
        Path file = new Path(filename);

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).build()) {
            Group group;
            int count = 0;
            double totalSalary = 0.0;
            int activeCount = 0;

            while ((group = reader.read()) != null) {
                int id = group.getInteger("id", 0);
                String name = group.getString("name", 0);
                double salary = group.getDouble("salary", 0);
                boolean active = group.getBoolean("active", 0);

                totalSalary += salary;
                if (active) {
                    activeCount++;
                }

                if (count < 5) {
                    System.out.printf("Employee %d: %s, Salary: $%.2f, Active: %s%n",
                        id, name, salary, active ? "Yes" : "No");
                }

                count++;
            }

            System.out.printf("%nStatistics:%n");
            System.out.printf("Total employees: %d%n", count);
            System.out.printf("Average salary: $%.2f%n", totalSalary / count);
            System.out.printf("Active employees: %d (%.1f%%)%n",
                activeCount, (activeCount * 100.0) / count);
        }
    }
}
