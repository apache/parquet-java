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
package org.apache.parquet.proto;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.proto.test.TestProtobuf.Document;
import org.apache.parquet.proto.test.TestProtobuf.Language;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.protobuf.MessageOrBuilder;

public class ProtoReadWriteTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testWriteReadProto() throws IOException {
    final File tmp = this.folder.newFolder();
    final Path path =
        new Path(new File(tmp, UUID.randomUUID().toString()).getPath());

    final List<Document> documents = Arrays.asList(
        Document.newBuilder().setDocId(1L)
            .addName(Document.Name.newBuilder()
                .addName(Language.newBuilder().setCode("AA").build()))
            .build(),
        Document.newBuilder().setDocId(2L)
            .addName(Document.Name.newBuilder().addName(
                Language.newBuilder().setCode("BB").build()))
            .build(),
        Document.newBuilder().setDocId(3L).addName(Document.Name.newBuilder()
            .addName(Language.newBuilder().setCode("CC").build())).build());

    try (ParquetWriter<Document> writer = ProtoParquetWriter
        .builder(path, Document.getDefaultInstance()).build()) {
      for (Document document : documents) {
        writer.write(document);
      }
    }

    List<Document> results = new ArrayList<>();
    try (ParquetReader<Document> reader = ProtoParquetReader
        .builder(path, Document.getDefaultInstance()).build()) {
      Iterator<Document> iter = ProtoParquetIterator.wrap(reader);
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertEquals(documents, results);
  }

  @Test
  public void testWriteReadMessage() throws IOException {
    final File tmp = this.folder.newFolder();
    final Path path =
        new Path(new File(tmp, UUID.randomUUID().toString()).getPath());

    final List<Document> documents = Arrays.asList(
        Document.newBuilder().setDocId(1L)
            .addName(Document.Name.newBuilder()
                .addName(Language.newBuilder().setCode("AA").build()))
            .build(),
        Document.newBuilder().setDocId(2L)
            .addName(Document.Name.newBuilder().addName(
                Language.newBuilder().setCode("BB").build()))
            .build(),
        Document.newBuilder().setDocId(3L).addName(Document.Name.newBuilder()
            .addName(Language.newBuilder().setCode("CC").build())).build());

    try (ParquetWriter<MessageOrBuilder> writer = ProtoParquetWriter
        .builder(path).withMessage(Document.getDefaultInstance()).build()) {
      for (Document document : documents) {
        writer.write(document);
      }
    }

    List<Document> results = new ArrayList<>();
    try (ParquetReader<MessageOrBuilder> reader =
        ProtoParquetReader.builder(path).build()) {
      Iterator<Document> iter = ProtoParquetMessageIterator.wrap(reader);
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertEquals(documents, results);
  }
}
