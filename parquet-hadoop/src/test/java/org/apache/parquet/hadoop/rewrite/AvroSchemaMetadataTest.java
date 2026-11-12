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
package org.apache.parquet.hadoop.rewrite;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class AvroSchemaMetadataTest {

  private static final String AVRO_SCHEMA_KEY = "parquet.avro.schema";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** The example of the issue: a required field is pruned. */
  private static final String PEOPLE_AVRO_SCHEMA = "{\"type\":\"record\",\"name\":\"people\",\"fields\":["
      + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"ssn\",\"type\":\"string\"}]}";

  private static final MessageType PEOPLE_SCHEMA = MessageTypeParser.parseMessageType(
      "message people { optional int64 id; optional binary name (STRING); required binary ssn (STRING); }");

  @TempDir
  private java.nio.file.Path tempDir;

  @Test
  public void testPruneTopLevelField() throws Exception {
    MessageType pruned = MessageTypeParser.parseMessageType(
        "message people { optional int64 id; optional binary name (STRING); }");

    assertSameJson(
        AvroSchemaMetadata.adjust(PEOPLE_AVRO_SCHEMA, pruned, Collections.emptyMap()),
        "{\"type\":\"record\",\"name\":\"people\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
  }

  @Test
  public void testRenameTopLevelField() throws Exception {
    assertSameJson(
        AvroSchemaMetadata.adjust(
            PEOPLE_AVRO_SCHEMA, PEOPLE_SCHEMA, Collections.singletonMap("name", "full_name")),
        "{\"type\":\"record\",\"name\":\"people\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"full_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"ssn\",\"type\":\"string\"}]}");
  }

  @Test
  public void testPruneNestedFields() throws Exception {
    String avroSchema = "{\"type\":\"record\",\"name\":\"r\",\"namespace\":\"ns\",\"fields\":["
        + "{\"name\":\"opt\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Opt\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}]},"
        + "{\"name\":\"list3\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"L3\","
        + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}}},"
        + "{\"name\":\"list2\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"L2\","
        + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}}},"
        + "{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"M\","
        + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}}},"
        + "{\"name\":\"union\",\"type\":[\"null\",\"int\",{\"type\":\"record\",\"name\":\"U\","
        + "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}]},"
        + "{\"name\":\"date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}}]}";
    // The "b" fields are pruned
    MessageType pruned = MessageTypeParser.parseMessageType("message r {"
        + " optional group opt { required int32 a; }"
        + " required group list3 (LIST) { repeated group list { required group element { required int32 a; } } }"
        + " required group list2 (LIST) { repeated group array { required int32 a; } }"
        + " required group map (MAP) { repeated group key_value { required binary key (STRING);"
        + " required group value { required int32 a; } } }"
        + " optional group union { optional int32 member0; optional group member1 { required int32 a; } }"
        + " required int32 date (DATE); }");

    assertSameJson(
        AvroSchemaMetadata.adjust(avroSchema, pruned, Collections.emptyMap()),
        avroSchema.replace(",{\"name\":\"b\",\"type\":\"int\"}", ""));
  }

  @Test
  public void testRemoveSchemaIfRecordIsReusedByName() {
    String avroSchema = "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
        + "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
        + "{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"zip\",\"type\":\"string\"}]}},"
        + "{\"name\":\"work\",\"type\":\"Address\"}]}";
    MessageType pruned = MessageTypeParser.parseMessageType("message r {"
        + " required group home { required binary city (STRING); }"
        + " required group work { required binary city (STRING); required binary zip (STRING); } }");

    assertThat(AvroSchemaMetadata.adjust(metadata(avroSchema), pruned, Collections.emptyMap()))
        .doesNotContainKey(AVRO_SCHEMA_KEY)
        .containsEntry("other", "value");
  }

  @Test
  public void testRemoveSchemaIfParquetFieldIsMissingInAvroSchema() {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message people { optional int64 id; optional binary name (STRING); optional int32 age; }");

    assertThat(AvroSchemaMetadata.adjust(metadata(PEOPLE_AVRO_SCHEMA), schema, Collections.emptyMap()))
        .doesNotContainKey(AVRO_SCHEMA_KEY)
        .containsEntry("other", "value");
  }

  @Test
  public void testRemoveInvalidSchema() {
    assertThat(AvroSchemaMetadata.adjust(metadata("{not json"), PEOPLE_SCHEMA, Collections.emptyMap()))
        .doesNotContainKey(AVRO_SCHEMA_KEY)
        .containsEntry("other", "value");
  }

  @Test
  public void testLegacyAvroSchemaKey() throws Exception {
    MessageType pruned = MessageTypeParser.parseMessageType(
        "message people { optional int64 id; optional binary name (STRING); }");
    Map<String, String> metadata = Collections.singletonMap("avro.schema", PEOPLE_AVRO_SCHEMA);

    Map<String, String> adjusted = AvroSchemaMetadata.adjust(metadata, pruned, Collections.emptyMap());

    assertThat(adjusted).containsKey("avro.schema");
    assertSameJson(
        adjusted.get("avro.schema"),
        AvroSchemaMetadata.adjust(PEOPLE_AVRO_SCHEMA, pruned, Collections.emptyMap()));
    assertThat(json(adjusted.get("avro.schema")).toString()).doesNotContain("ssn");
  }

  @Test
  public void testRewriterPrunesAvroSchema() throws Exception {
    Path input = writePeople();
    Path output = new Path(tempDir.resolve("pruned.parquet").toUri());
    Configuration conf = new Configuration();
    RewriteOptions options = new RewriteOptions.Builder(conf, input, output)
        .prune(Collections.singletonList("ssn"))
        .build();
    try (ParquetRewriter rewriter = new ParquetRewriter(options)) {
      rewriter.processBlocks();
    }

    Map<String, String> metadata = keyValueMetaData(output);
    assertThat(metadata).containsKey(AVRO_SCHEMA_KEY).containsEntry("other", "value");
    assertSameJson(
        metadata.get(AVRO_SCHEMA_KEY),
        "{\"type\":\"record\",\"name\":\"people\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
  }

  @Test
  public void testRewriterRenamesAvroSchemaFields() throws Exception {
    Path input = writePeople();
    Path output = new Path(tempDir.resolve("renamed.parquet").toUri());
    Configuration conf = new Configuration();
    RewriteOptions options = new RewriteOptions.Builder(conf, input, output)
        .renameColumns(Collections.singletonMap("name", "full_name"))
        .build();
    try (ParquetRewriter rewriter = new ParquetRewriter(options)) {
      rewriter.processBlocks();
    }

    assertSameJson(
        keyValueMetaData(output).get(AVRO_SCHEMA_KEY),
        "{\"type\":\"record\",\"name\":\"people\",\"fields\":["
            + "{\"name\":\"id\",\"type\":[\"null\",\"long\"],\"default\":null},"
            + "{\"name\":\"full_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"ssn\",\"type\":\"string\"}]}");
  }

  @Test
  public void testRewriterKeepsAvroSchemaWithoutPruningOrRenaming() throws Exception {
    Path input = writePeople();
    Path output = new Path(tempDir.resolve("copied.parquet").toUri());
    Configuration conf = new Configuration();
    RewriteOptions options = new RewriteOptions.Builder(conf, input, output).build();
    try (ParquetRewriter rewriter = new ParquetRewriter(options)) {
      rewriter.processBlocks();
    }

    assertThat(keyValueMetaData(output)).containsEntry(AVRO_SCHEMA_KEY, PEOPLE_AVRO_SCHEMA);
  }

  private Path writePeople() throws IOException {
    Path path = new Path(tempDir.resolve("people.parquet").toUri());
    SimpleGroupFactory factory = new SimpleGroupFactory(PEOPLE_SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(PEOPLE_SCHEMA)
        .withExtraMetaData(metadata(PEOPLE_AVRO_SCHEMA))
        .build()) {
      writer.write(
          factory.newGroup().append("id", 1L).append("name", "Alice").append("ssn", "111-22-3333"));
      writer.write(
          factory.newGroup().append("id", 2L).append("name", "Bob").append("ssn", "444-55-6666"));
    }
    return path;
  }

  private static Map<String, String> keyValueMetaData(Path path) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
      return reader.getFooter().getFileMetaData().getKeyValueMetaData();
    }
  }

  private static Map<String, String> metadata(String avroSchema) {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(AVRO_SCHEMA_KEY, avroSchema);
    metadata.put("other", "value");
    return metadata;
  }

  private static JsonNode json(String json) throws IOException {
    return MAPPER.readTree(json);
  }

  private static void assertSameJson(String actual, String expected) throws IOException {
    assertThat(json(actual)).isEqualTo(json(expected));
  }
}
