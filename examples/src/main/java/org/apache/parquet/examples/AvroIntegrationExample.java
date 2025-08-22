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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Random;

/**
 * Example demonstrating Avro integration with Parquet.
 *
 * This example shows:
 * 1. How to define an Avro schema
 * 2. How to write Avro records to Parquet format
 * 3. How to read Parquet files as Avro records
 * 4. How to use different Avro data models (Generic vs Specific)
 */
public class AvroIntegrationExample {

    public static void main(String[] args) throws IOException {
        String outputFile = "data/products.parquet";

        String schemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"Product\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"int\"},\n" +
            "    {\"name\": \"name\", \"type\": \"string\"},\n" +
            "    {\"name\": \"category\", \"type\": \"string\"},\n" +
            "    {\"name\": \"price\", \"type\": \"double\"},\n" +
            "    {\"name\": \"inStock\", \"type\": \"boolean\"},\n" +
            "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}}\n" +
            "  ]\n" +
            "}";

        Schema schema = new Schema.Parser().parse(schemaJson);

        writeProductData(outputFile, schema);

        readProductData(outputFile);

        readProductDataWithProjection(outputFile);
    }

    private static void writeProductData(String filename, Schema schema) throws IOException {
        Path file = new Path(filename);

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(file)
            .withSchema(schema)
            .withDataModel(GenericData.get())
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withRowGroupSize(64 * 1024 * 1024)
            .withPageSize(1024 * 1024)
            .withDictionaryEncoding(true)
            .build()) {

            Random random = new Random(42);
            String[] categories = {"Electronics", "Clothing", "Books", "Home", "Sports"};
            String[] tagSets = {
                "new,featured",
                "sale,discount",
                "premium,quality",
                "popular,bestseller",
                "limited,exclusive"
            };

            for (int i = 0; i < 500; i++) {
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", i);
                record.put("name", "Product " + i);
                record.put("category", categories[random.nextInt(categories.length)]);
                record.put("price", 10.0 + random.nextDouble() * 990.0);
                record.put("inStock", random.nextBoolean());

                String[] tags = tagSets[random.nextInt(tagSets.length)].split(",");
                record.put("tags", java.util.Arrays.asList(tags));

                writer.write(record);
            }

            System.out.println("Successfully wrote 500 product records");
        }
    }

    private static void readProductData(String filename) throws IOException {
        Path file = new Path(filename);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
            .withDataModel(GenericData.get())
            .build()) {

            GenericRecord record;
            int count = 0;
            double totalPrice = 0.0;
            int inStockCount = 0;

            while ((record = reader.read()) != null) {
                Integer id = (Integer) record.get("id");
                String name = record.get("name").toString();
                String category = record.get("category").toString();
                Double price = (Double) record.get("price");
                Boolean inStock = (Boolean) record.get("inStock");
                @SuppressWarnings("unchecked")
                java.util.List<?> tagsList = (java.util.List<?>) record.get("tags");
                java.util.List<String> tags = new java.util.ArrayList<>();
                for (Object tag : tagsList) {
                    tags.add(tag.toString());
                }

                totalPrice += price;
                if (inStock) {
                    inStockCount++;
                }

                if (count < 3) {
                    System.out.printf("Product %d: %s (%s), Price: $%.2f, In Stock: %s, Tags: %s%n",
                        id, name, category, price, inStock ? "Yes" : "No", tags);
                }

                count++;
            }

            System.out.printf("%nStatistics:%n");
            System.out.printf("Total products: %d%n", count);
            System.out.printf("Average price: $%.2f%n", totalPrice / count);
            System.out.printf("Products in stock: %d (%.1f%%)%n",
                inStockCount, (inStockCount * 100.0) / count);
        }
    }

    private static void readProductDataWithProjection(String filename) throws IOException {
        Path file = new Path(filename);

        String projectionSchemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ProductProjection\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"int\"},\n" +
            "    {\"name\": \"name\", \"type\": \"string\"}\n" +
            "  ]\n" +
            "}";

        Schema projectionSchema = new Schema.Parser().parse(projectionSchemaJson);

        // Note: withProjection is not available in this version, using configuration instead
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("parquet.avro.projection", projectionSchemaJson);

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
            .withDataModel(GenericData.get())
            .withConf(conf)
            .build()) {

            GenericRecord record;
            int count = 0;

            while ((record = reader.read()) != null && count < 5) {
                Integer id = (Integer) record.get("id");
                String name = record.get("name").toString();

                System.out.printf("Product %d: %s%n", id, name);
                count++;
            }

            System.out.println("... (showing only first 5 records with projection)");
        }
    }
}
