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
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

/**
 * Schema, rows and keys of the DataPageV2 encryption tests and fixtures. {@code seq} is required (no
 * levels), {@code id} and {@code name} are optional (definition levels), {@code tags} is a list
 * (repetition and definition levels).
 */
final class DataPageV2EncryptionTestData {

  public static final int NUM_ROWS = 500;

  /** Footer key, also the key of all columns with uniform encryption. */
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

  /** Builds row {@code i}, as stored in the {@code arrow-v2-*} fixtures. */
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

  public static final byte[] COLUMN_KEY = "fedcba9876543210".getBytes(StandardCharsets.UTF_8);

  public static final byte[] COLUMN_KEY_METADATA = "colkey".getBytes(StandardCharsets.UTF_8);

  /** Resolves the key metadata of {@link #FOOTER_KEY} and {@link #COLUMN_KEY}. */
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
   * Reads all rows as strings. Page checksums are verified unless {@code conf} disables it.
   *
   * @param conf not modified
   * @param decryption the decryption properties, or null to use the ones configured in {@code conf}
   * @param directMemory whether to read into direct buffers and decrypt off heap
   */
  public static List<String> readRows(
      Path file, Configuration conf, FileDecryptionProperties decryption, boolean directMemory)
      throws IOException {
    return readRows(file, conf, decryption, directMemory, FilterCompat.NOOP);
  }

  /** Like {@link #readRows(Path, Configuration, FileDecryptionProperties, boolean)}, filtered by page. */
  public static List<String> readRows(
      Path file,
      Configuration conf,
      FileDecryptionProperties decryption,
      boolean directMemory,
      FilterCompat.Filter filter)
      throws IOException {
    Configuration readConf = new Configuration(conf);
    readConf.setBoolean(ParquetInputFormat.OFF_HEAP_DECRYPT_BUFFER_ENABLED, directMemory);
    readConf.setBoolean(
        ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED,
        conf.getBoolean(ParquetInputFormat.PAGE_VERIFY_CHECKSUM_ENABLED, true));
    if (filter != FilterCompat.NOOP) {
      // Return every row of the pages that are read, so the test can see which pages were skipped
      readConf.setBoolean(ParquetInputFormat.RECORD_FILTERING_ENABLED, false);
    }
    List<String> rows = new ArrayList<>();
    // close() fails on leaked buffers
    try (TrackingByteBufferAllocator allocator = TrackingByteBufferAllocator.wrap(
        directMemory ? new DirectByteBufferAllocator() : new HeapByteBufferAllocator())) {
      ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), file)
          .withConf(readConf)
          .withFilter(filter)
          .withAllocator(allocator);
      if (decryption != null) {
        builder.withDecryption(decryption);
      }
      try (ParquetReader<Group> reader = builder.build()) {
        Group group;
        while ((group = reader.read()) != null) {
          rows.add(group.toString());
        }
      }
    }
    return rows;
  }
}
