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
package org.apache.parquet.cli;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

public class ShowSizeStatisticsCliTest extends CliTestBase {

  private final int numRecord = 10000;

  @Test
  public void showSizeStatistics() throws Exception {
    File file = createParquetFileWithStats();

    cli("size-stats " + file.getAbsolutePath())
        .ok()
        .matchOutputFromFile("src/test/resources/cli-outputs/size-stats.txt");
  }

  private File createParquetFileWithStats() throws IOException {
    MessageType schema = new MessageType(
        "schema",
        new PrimitiveType(REQUIRED, INT64, "DocId"),
        new PrimitiveType(REQUIRED, INT32, "CategoryId"),
        new PrimitiveType(OPTIONAL, BOOLEAN, "IsActive"),
        new PrimitiveType(REPEATED, FLOAT, "Prices"),
        new PrimitiveType(REPEATED, BINARY, "Tags"),
        new PrimitiveType(REQUIRED, BINARY, "ProductName"),
        new PrimitiveType(OPTIONAL, BINARY, "Description"),
        new PrimitiveType(REQUIRED, FIXED_LEN_BYTE_ARRAY, 16, "UUID"));

    Configuration conf = new Configuration();
    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    File file = new File(getTempFolder(), "test.parquet");
    String filePath = file.getAbsolutePath();
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(filePath))
        .withType(schema)
        .withSizeStatisticsEnabled(true)
        .withPageRowCountLimit(50)
        .withMinRowCountForPageSizeCheck(5)
        .withDictionaryEncoding(true)
        .withValidation(false)
        .withConf(conf);

    Random rnd = new Random(42);
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);

        g.add("DocId", rnd.nextLong());

        g.add("CategoryId", rnd.nextInt(100));

        // Operations to generate some non null meaningful test statistics on the parquet file.
        if (i % 4 != 0) {
          g.add("IsActive", rnd.nextBoolean());
        }

        int priceCount = rnd.nextInt(4);
        for (int p = 0; p < priceCount; p++) {
          g.add("Prices", rnd.nextFloat() * 1000);
        }

        String[] possibleTags = {"electronics", "bestseller", "new", "discount", "premium"};
        int tagCount = rnd.nextInt(5);
        for (int t = 0; t < tagCount; t++) {
          g.add("Tags", Binary.fromString(possibleTags[rnd.nextInt(possibleTags.length)]));
        }

        String[] products = {
          "Laptop",
          "Mouse",
          "Keyboard",
          "Monitor",
          "Headphones",
          "Smartphone",
          "Tablet",
          "Camera",
          "Printer",
          "Speaker"
        };
        g.add("ProductName", Binary.fromString(products[i % products.length] + "_Model_" + (i % 50)));

        if (i % 3 != 0) {
          StringBuilder desc = new StringBuilder();
          desc.append("Product description for item ").append(i).append(": ");
          int descLength = rnd.nextInt(200) + 50;
          for (int j = 0; j < descLength; j++) {
            desc.append((char) ('a' + rnd.nextInt(26)));
          }
          g.add("Description", Binary.fromString(desc.toString()));
        }

        byte[] uuid = new byte[16];
        rnd.nextBytes(uuid);
        g.add("UUID", Binary.fromConstantByteArray(uuid));

        writer.write(g);
      }
    }

    return file;
  }

  @Test
  public void showsHelpMessage() throws Exception {
    cli("help size-stats").ok().matchOutputFromFile("src/test/resources/cli-outputs/size-stats-help.txt");
  }
}
