/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestReadWriteMapKeyValue {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testMapLogicalType() throws Exception {
    MessageType messageType = MessageTypeParser.parseMessageType(
      "message example {\n" +
        "  required group testMap (MAP) {\n" +
        "    repeated group key_value {\n" +
        "      required binary key (STRING);\n" +
        "      required int64 value;\n" +
        "    }\n" +
        "  }\n" +
        "}"
    );

    verifyMap(messageType, "key_value");
  }

  @Test
  public void testMapConvertedType() throws Exception {
    MessageType messageType = MessageTypeParser.parseMessageType(
      "message example {\n" +
        "  required group testMap (MAP) {\n" +
        "    repeated group map (MAP_KEY_VALUE) {\n" +
        "      required binary key (STRING);\n" +
        "      required int64 value;\n" +
        "    }\n" +
        "  }\n" +
        "}"
    );

    verifyMap(messageType, "map");
  }

  private void verifyMap(final MessageType messageType, final String keyValueName) throws IOException {
    Path file = new Path(folder.newFolder("testReadWriteMapKeyValue").getPath(), keyValueName + ".parquet");

    try (ParquetWriter<Group> writer =
           ExampleParquetWriter
             .builder(file)
             .withType(messageType)
             .build()) {
      final Group group = new SimpleGroup(messageType);
      final Group mapGroup = group.addGroup("testMap");

      for (int index = 0; index < 5; index++) {
        final Group keyValueGroup = mapGroup.addGroup(keyValueName);
        keyValueGroup.add("key", Binary.fromString("key" + index));
        keyValueGroup.add("value", 100L + index);
      }

      writer.write(group);
    }

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).build()) {
      Group group = reader.read();

      assertNotNull(group);
      assertEquals(5, group.getGroup("testMap", 0).getFieldRepetitionCount(keyValueName));

      for (int index = 0; index < 5; index++) {
        assertEquals("key" + index, group.getGroup("testMap", 0).getGroup(keyValueName, index).getString("key", 0));
        assertEquals(100L + index, group.getGroup("testMap", 0).getGroup(keyValueName, index).getLong("value", 0));
      }
    }
  }
}
