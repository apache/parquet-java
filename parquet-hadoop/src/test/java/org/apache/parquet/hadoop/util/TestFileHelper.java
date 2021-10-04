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
package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

public class TestFileHelper {

  public static MessageType schema = new MessageType("schema",
    new PrimitiveType(OPTIONAL, INT64, "DocId"),
    new PrimitiveType(REQUIRED, BINARY, "Name"),
    new PrimitiveType(OPTIONAL, BINARY, "Gender"),
    new GroupType(OPTIONAL, "Links",
      new PrimitiveType(REPEATED, BINARY, "Backward"),
      new PrimitiveType(REPEATED, BINARY, "Forward")));

  private static Random rnd = new Random(5);

  public static String createParquetFile(Configuration conf, Map<String, String> extraMeta, int numRecord, String prefix, String codec,
                                         ParquetProperties.WriterVersion writerVersion, int pageSize, TestDocs testDocs) throws IOException {
    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = createTempFile(prefix);
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file))
      .withConf(conf)
      .withWriterVersion(writerVersion)
      .withExtraMetaData(extraMeta)
      .withValidation(true)
      .withPageSize(pageSize)
      .withCompressionCodec(CompressionCodecName.valueOf(codec));
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", testDocs.docId[i]);
        g.add("Name", testDocs.name[i]);
        g.add("Gender", testDocs.gender[i]);
        Group links = g.addGroup("Links");
        links.add(0, testDocs.linkBackward[i]);
        links.add(1, testDocs.linkForward[i]);
        writer.write(g);
      }
    }
    return file;
  }

  private static long getLong() {
    return ThreadLocalRandom.current().nextLong(1000);
  }

  private static String getString() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append(chars[rnd.nextInt(10)]);
    }
    return sb.toString();
  }

  public static String createTempFile(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }

  public static List<ColumnPath> getPaths() {
    List<ColumnPath> paths = new ArrayList<>();
    paths.add(ColumnPath.fromDotString("DocId"));
    paths.add(ColumnPath.fromDotString("Name"));
    paths.add(ColumnPath.fromDotString("Gender"));
    paths.add(ColumnPath.fromDotString("Links.Backward"));
    paths.add(ColumnPath.fromDotString("Links.Forward"));
    return paths;
  }

  public static class TestDocs {
    public long[] docId;
    public String[] name;
    public String[] gender;
    public String[] linkBackward;
    public String[] linkForward;

    public TestDocs(int numRecord) {
      docId = new long[numRecord];
      for (int i = 0; i < numRecord; i++) {
        docId[i] = getLong();
      }

      name = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        name[i] = getString();
      }

      gender = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        gender[i] = getString();
      }

      linkBackward = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        linkBackward[i] = getString();
      }

      linkForward = new String[numRecord];
      for (int i = 0; i < numRecord; i++) {
        linkForward[i] = getString();
      }
    }
  }
}
