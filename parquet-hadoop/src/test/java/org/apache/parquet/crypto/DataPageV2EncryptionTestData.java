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

package org.apache.parquet.crypto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.DirectByteBufferAllocator;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Deterministic data, schema and keys shared by the DataPageV2 encryption tests and by the scripts
 * that generated the fixture files under {@code src/test/resources/encryption/datapagev2}.
 *
 * <p>The schema deliberately mixes a required column (no repetition / definition levels), optional
 * columns (definition levels only) and a nested list (repetition and definition levels), because
 * the layout of an encrypted DataPageV2 only differs between implementations when a page has
 * non-empty levels.
 *
 * <p>The row generator must stay in sync with {@code generate_arrow_fixtures.py} in the fixtures
 * directory.
 */
public final class DataPageV2EncryptionTestData {

  public static final int NUM_ROWS = 500;

  /** The footer key; with uniform encryption it is also used for all of the columns. */
  public static final byte[] FOOTER_KEY = "0123456789abcdef".getBytes(StandardCharsets.UTF_8);

  public static final byte[] FOOTER_KEY_METADATA = "footkey".getBytes(StandardCharsets.UTF_8);

  public static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message schema {"
      + "  required int64 seq;"
      + "  optional int32 id;"
      + "  optional binary name (STRING);"
      + "  optional group tags (LIST) {"
      + "    repeated group list {"
      + "      optional binary element (STRING);"
      + "    }"
      + "  }"
      + "}");

  private DataPageV2EncryptionTestData() {}

  /** Builds row {@code i}. Must match {@code row(i)} in {@code generate_arrow_fixtures.py}. */
  public static Group row(int i) {
    Group group = new SimpleGroupFactory(SCHEMA).newGroup();
    group.add("seq", (long) i);
    if (i % 7 != 0) {
      group.add("id", i);
    }
    if (i % 5 != 0) {
      group.add("name", "name-" + i);
    }
    if (i % 4 != 0) {
      Group tags = group.addGroup("tags");
      int count = i % 4;
      for (int j = 0; j < count; j++) {
        Group element = tags.addGroup("list");
        if ((i + j) % 3 != 0) {
          element.add("element", "tag-" + i + "-" + j);
        }
      }
    }
    return group;
  }

  public static List<String> expectedRows() {
    List<String> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      rows.add(row(i).toString());
    }
    return rows;
  }

  /** Key of the columns that are encrypted with their own key, in the tests that do that. */
  public static final byte[] COLUMN_KEY = "fedcba9876543210".getBytes(StandardCharsets.UTF_8);

  public static final byte[] COLUMN_KEY_METADATA = "colkey".getBytes(StandardCharsets.UTF_8);

  /** Decryption properties for the files that were written with {@link #FOOTER_KEY} and {@link #COLUMN_KEY}. */
  public static FileDecryptionProperties decryptionProperties() {
    return FileDecryptionProperties.builder()
        .withKeyRetriever(keyMetadata -> {
          String id = new String(keyMetadata, StandardCharsets.UTF_8);
          switch (id) {
            case "footkey":
              return FOOTER_KEY;
            case "colkey":
              return COLUMN_KEY;
            default:
              throw new KeyAccessDeniedException("Unknown key " + id);
          }
        })
        .build();
  }

  /**
   * Reads all of the rows of a file, rendered as strings.
   *
   * @param conf may hold the configuration of a decryption properties factory
   * @param decryption the decryption properties, or null to take them from the configuration
   * @param directMemory whether to read the file into direct buffers and decrypt it off-heap
   */
  public static List<String> readRows(
      Path file, Configuration conf, FileDecryptionProperties decryption, boolean directMemory)
      throws IOException {
    conf.setBoolean(ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED, directMemory);
    ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf)
        .withAllocator(directMemory ? new DirectByteBufferAllocator() : new HeapByteBufferAllocator());
    if (decryption != null) {
      builder.withDecryption(decryption);
    }
    List<String> rows = new ArrayList<>();
    try (ParquetReader<Group> reader = builder.build()) {
      Group group;
      while ((group = reader.read()) != null) {
        rows.add(group.toString());
      }
    }
    return rows;
  }
}
