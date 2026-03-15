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
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;

public class AdvancedFeaturesExample {

    public static void main(String[] args) throws IOException {
        String filename = "data/sales.parquet";

        MessageType schema = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("product")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("amount")
            .required(PrimitiveType.PrimitiveTypeName.BINARY).named("region")
            .named("sale");

        writeSalesData(filename, schema);

        testPredicatePushdown(filename);

        testPerformanceOptimization(filename);
    }

    private static void writeSalesData(String filename, MessageType schema) throws IOException {
        Path file = new Path(filename);

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        try (ParquetWriter<Group> writer = new ParquetWriter<>(
            file,                               // destination path
            new GroupWriteSupport(),            // write support implementation
            CompressionCodecName.SNAPPY,        // compression codec
            64 * 1024 * 1024,                   // row-group size
            1024 * 1024,                        // page size
            1024 * 1024,                        // dictionary page size
            true,                               // enable dictionary encoding
            false,                              // disable validation
            ParquetWriter.DEFAULT_WRITER_VERSION, // writer version
            conf)) {

            SimpleGroupFactory factory = new SimpleGroupFactory(schema);
            Random random = new Random(42);
            String[] products = {"Laptop", "Phone", "Tablet", "Monitor", "Keyboard"};
            String[] regions = {"North", "South", "East", "West"};

            for (int i = 0; i < 10000; i++) {
                Group group = factory.newGroup()
                    .append("id", i)
                    .append("product", products[random.nextInt(products.length)])
                    .append("amount", 100.0 + random.nextDouble() * 900.0)
                    .append("region", regions[random.nextInt(regions.length)]);
                writer.write(group);
            }

            System.out.println("Wrote 10000 sales records");
        }
    }

    private static void testPredicatePushdown(String filename) throws IOException {
        Path file = new Path(filename);

        Operators.DoubleColumn amountColumn = FilterApi.doubleColumn("amount");
        FilterPredicate amountFilter = FilterApi.gt(amountColumn, 500.0);

        long startTime = System.currentTimeMillis();
        int count = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
            .withFilter(FilterCompat.get(amountFilter))
            .build()) {

            Group group;
            while ((group = reader.read()) != null) {
                count++;
            }
        }
        long filterTime = System.currentTimeMillis() - startTime;

        Operators.BinaryColumn regionColumn = FilterApi.binaryColumn("region");
        FilterPredicate complexFilter = FilterApi.and(
            FilterApi.gt(amountColumn, 500.0),
            FilterApi.eq(regionColumn, org.apache.parquet.io.api.Binary.fromString("North"))
        );

        startTime = System.currentTimeMillis();
        count = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
            .withFilter(FilterCompat.get(complexFilter))
            .build()) {

            Group group;
            while ((group = reader.read()) != null) {
                count++;
            }
        }
        filterTime = System.currentTimeMillis() - startTime;
        System.out.printf("Found %d records in %dms%n", count, filterTime);
    }

    private static void testPerformanceOptimization(String filename) throws IOException {
        Path file = new Path(filename);

        System.out.println("Testing default reading performance...");
        long startTime = System.currentTimeMillis();
        int count = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).build()) {
            Group group;
            while ((group = reader.read()) != null) {
                count++;
            }
        }
        long defaultTime = System.currentTimeMillis() - startTime;
        System.out.printf("Default: Read %d records in %dms%n", count, defaultTime);

        System.out.println("Testing projection reading performance...");
        MessageType projection = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32).named("id")
            .required(PrimitiveType.PrimitiveTypeName.DOUBLE).named("amount")
            .named("sale");

        startTime = System.currentTimeMillis();
        count = 0;
        // Note: withProjection is not available in this version, using configuration instead
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.read.schema", projection.toString());

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
            .withConf(conf)
            .build()) {

            Group group;
            while ((group = reader.read()) != null) {
                count++;
            }
        }
        long projectionTime = System.currentTimeMillis() - startTime;
        System.out.printf("Projection: Read %d records in %dms (%.1fx faster)%n",
            count, projectionTime, (double) defaultTime / projectionTime);
    }
}
