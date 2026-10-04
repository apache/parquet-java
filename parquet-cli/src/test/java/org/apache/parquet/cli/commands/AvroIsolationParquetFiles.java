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
package org.apache.parquet.cli.commands;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public final class AvroIsolationParquetFiles {
  static final String LIST_OF_LISTS = "listOfLists";

  private static final MessageType AVRO_COMPAT_LIST_IN_LIST_SCHEMA =
      MessageTypeParser.parseMessageType("message AvroCompatListInList {\n"
          + "  optional group listOfLists (LIST) {\n"
          + "    repeated group array (LIST) {\n"
          + "      repeated int32 array;\n"
          + "    }\n"
          + "  }\n"
          + "}\n");

  private static final MessageType NESTED_INT96_SCHEMA =
      MessageTypeParser.parseMessageType("message NativeCompatInt96 {\n"
          + "  optional group event {\n"
          + "    required int32 id;\n"
          + "    required int96 legacy_ts;\n"
          + "  }\n"
          + "}\n");

  private AvroIsolationParquetFiles() {}

  static File writeAvroCompatListInList(File file) throws IOException {
    SimpleGroupFactory factory = new SimpleGroupFactory(AVRO_COMPAT_LIST_IN_LIST_SCHEMA);

    Group record = factory.newGroup();
    Group listOfLists = record.addGroup(LIST_OF_LISTS);
    appendIntList(listOfLists.addGroup("array"), 34, 35, 36);
    listOfLists.addGroup("array");
    appendIntList(listOfLists.addGroup("array"), 32, 33, 34);

    write(file, AVRO_COMPAT_LIST_IN_LIST_SCHEMA, record);
    return file;
  }

  public static File writeNestedInt96(File file) throws IOException {
    SimpleGroupFactory factory = new SimpleGroupFactory(NESTED_INT96_SCHEMA);

    Group record = factory.newGroup();
    record.addGroup("event")
        .append("id", 1)
        .append("legacy_ts", Binary.fromConstantByteArray(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}));

    write(file, NESTED_INT96_SCHEMA, record);
    return file;
  }

  private static void appendIntList(Group list, int... values) {
    for (int value : values) {
      list.append("array", value);
    }
  }

  private static void write(File file, MessageType schema, Group... records) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(schema, conf);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
        .withConf(conf)
        .withType(schema)
        .build()) {
      for (Group record : records) {
        writer.write(record);
      }
    }
  }
}
