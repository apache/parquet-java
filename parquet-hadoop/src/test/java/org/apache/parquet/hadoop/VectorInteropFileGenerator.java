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
package org.apache.parquet.hadoop;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE;
import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.CREATE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.MAX_PADDING_SIZE_DEFAULT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesWriter;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.LogicalTypes;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public final class VectorInteropFileGenerator {
  private VectorInteropFileGenerator() {}

  static MessageType mixedSchema() {
    return MessageTypeParser.parseMessageType("message m {"
        + " required int32 id;"
        + " required group f (VECTOR(5)) { repeated group list { required float element; } }"
        + " optional group o (VECTOR(2)) { repeated group list { required int32 element; } }"
        + " optional group a { required group h (VECTOR(2)) { repeated group list {"
        + " required fixed_len_byte_array(2) element (FLOAT16); } } }"
        + " repeated group outer { required group d (VECTOR(2)) { repeated group list {"
        + " required double element; } } }"
        + " required group b (VECTOR(2)) { repeated group list { required boolean element; } }"
        + " required group l (VECTOR(1)) { repeated group list { required int64 element; } }"
        + " required group s (VECTOR(1)) { repeated group list {"
        + " required int32 element (INTEGER(16,true)); } }"
        + " required group u (VECTOR(1)) { repeated group list {"
        + " required int64 element (INTEGER(64,false)); } }"
        + " required group dec (VECTOR(1)) { repeated group list {"
        + " required int32 element (DECIMAL(9,2)); } }"
        + " required group bytes (VECTOR(1)) { repeated group list {"
        + " required fixed_len_byte_array(4) element (DECIMAL(9,2)); } } }");
  }

  static List<Group> mixedRows() {
    List<Group> rows = new ArrayList<>();
    SimpleGroupFactory factory = new SimpleGroupFactory(mixedSchema());
    for (int i = 0; i < 6; i++) {
      Group row = factory.newGroup().append("id", i);
      Group f = row.addGroup("f");
      for (float value : new float[] {i + 0.5f, -0.0f, Float.MIN_VALUE, -0.25f, 100.0f}) {
        f.addGroup("list").append("element", value);
      }
      if (i % 2 != 0) {
        row.addGroup("o").addGroup("list").append("element", 7);
        row.getGroup("o", 0).addGroup("list").append("element", 8);
      }
      if (i != 1 && i != 4) {
        Group h = row.addGroup("a").addGroup("h");
        h.addGroup("list").append("element", Binary.fromConstantByteArray(new byte[] {0, (byte) 0x80}));
        h.addGroup("list").append("element", Binary.fromConstantByteArray(new byte[] {1, 0}));
      }
      for (int j = 0; j < i % 3; j++) {
        Group d = row.addGroup("outer").addGroup("d");
        d.addGroup("list").append("element", -0.0d);
        d.addGroup("list").append("element", Double.MIN_VALUE);
      }
      Group b = row.addGroup("b");
      b.addGroup("list").append("element", true);
      b.addGroup("list").append("element", false);
      row.addGroup("l").addGroup("list").append("element", 9L);
      row.addGroup("s").addGroup("list").append("element", -7);
      row.addGroup("u").addGroup("list").append("element", 9L);
      row.addGroup("dec").addGroup("list").append("element", 125);
      row.addGroup("bytes")
          .addGroup("list")
          .append("element", Binary.fromConstantByteArray(new byte[] {0, 0, 0x7c, 0}));
      rows.add(row);
    }
    return rows;
  }

  static void writeMixed(Path path, WriterVersion version, boolean dictionary) throws IOException {
    writeRows(path, mixedSchema(), version, dictionary, mixedRows());
  }

  static void writeRows(Path path, MessageType schema, WriterVersion version, boolean dictionary, List<Group> rows)
      throws IOException {
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withType(schema)
        .withWriterVersion(version)
        .withDictionaryEncoding(dictionary)
        .withValidation(false)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageRowCountLimit(2)
        .withMinRowCountForPageSizeCheck(1)
        .withMaxRowCountForPageSizeCheck(1)
        .build()) {
      for (Group row : rows) writer.write(row);
    }
  }

  static MessageType invalidSchema() {
    return MessageTypeParser.parseMessageType("message m { required int32 id;"
        + " required group short_v (LIST) { repeated group list { required float element; } }"
        + " required group long_v (LIST) { repeated group list { required float element; } }"
        + " optional group empty_v (LIST) { repeated group list { required float element; } }"
        + " repeated group nested { required group v (LIST) { repeated group list {"
        + " required float element; } } }"
        + " repeated group partial { required group v (LIST) { repeated group list {"
        + " required float element; } } }"
        + " required group nf_f (LIST) { repeated group list { required float element; } }"
        + " required group nf_d (LIST) { repeated group list { required double element; } }"
        + " required group nf_h (LIST) { repeated group list {"
        + " required fixed_len_byte_array(2) element (FLOAT16); } } }");
  }

  static List<Group> invalidRows() {
    MessageType schema = invalidSchema();
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    List<Group> rows = new ArrayList<>();
    int[] shortLengths = {4, 6, 5, 5};
    int[] longLengths = {6, 4, 5, 5};
    int[] emptyLengths = {0, 5, 5, 5};
    int[] partialLengths = {5, 5, 5, 3};
    byte[] halfNegativeInfinity = {0, (byte) 0xfc};
    byte[] halfZero = {0, 0};
    byte[] halfOne = {0, 0x3c};
    for (int i = 0; i < 4; i++) {
      Group row = factory.newGroup().append("id", i);
      for (String name : List.of("short_v", "long_v", "empty_v")) {
        int length = name.equals("short_v")
            ? shortLengths[i]
            : name.equals("long_v") ? longLengths[i] : emptyLengths[i];
        Group vector = row.addGroup(name);
        for (int k = 0; k < length; k++) {
          vector.addGroup("list").append("element", (float) (100 * i + k));
        }
      }
      int[] nestedLengths = i == 0 ? new int[] {4, 6} : i == 1 ? new int[0] : new int[] {5};
      for (int j = 0; j < nestedLengths.length; j++) {
        Group vector = row.addGroup("nested").addGroup("v");
        for (int k = 0; k < nestedLengths[j]; k++) {
          vector.addGroup("list").append("element", (float) (100 * i + 10 * j + k));
        }
      }
      Group partial = row.addGroup("partial").addGroup("v");
      for (int k = 0; k < partialLengths[i]; k++) {
        partial.addGroup("list").append("element", (float) (100 * i + k));
      }
      Group nfFloat = row.addGroup("nf_f");
      Group nfDouble = row.addGroup("nf_d");
      Group nfHalf = row.addGroup("nf_h");
      for (int k = 0; k < 8; k++) {
        nfFloat.addGroup("list").append("element", i == 0 && k == 0 ? Float.NaN : k == 0 ? 0.0f : 1.0f);
        nfDouble.addGroup("list")
            .append("element", i == 0 && k == 0 ? Double.POSITIVE_INFINITY : k == 0 ? 0.0d : 1.0d);
        nfHalf.addGroup("list")
            .append(
                "element",
                Binary.fromConstantByteArray(
                    i == 0 && k == 0 ? halfNegativeInfinity : k == 0 ? halfZero : halfOne));
      }
      rows.add(row);
    }
    return rows;
  }

  static void writeInvalid(Path path) throws IOException {
    writeRows(path, invalidSchema(), WriterVersion.PARQUET_1_0, true, invalidRows());
    rewriteFooter(path, footer -> {
      footer.getSchema().stream()
          .filter(element -> element.getConverted_type() == ConvertedType.LIST)
          .forEach(element -> element.setLogicalType(
              LogicalTypes.VECTOR(element.getName().startsWith("nf_") ? 8 : 5)));
      for (org.apache.parquet.format.ColumnChunk chunk :
          footer.getRow_groups().get(0).getColumns()) {
        chunk.getMeta_data().getStatistics().unsetNull_count();
        chunk.unsetColumn_index_offset();
        chunk.unsetColumn_index_length();
        chunk.unsetOffset_index_offset();
        chunk.unsetOffset_index_length();
      }
    });
  }

  static org.apache.parquet.format.FileMetaData readFooter(Path path) throws IOException {
    java.nio.file.Path local = java.nio.file.Path.of(path.toUri());
    byte[] original = Files.readAllBytes(local);
    int footerLength = BytesUtils.readIntLittleEndian(original, original.length - 8);
    int footerStart = original.length - 8 - footerLength;
    return Util.readFileMetaData(new ByteArrayInputStream(original, footerStart, footerLength));
  }

  static void rewriteFooter(Path path, Consumer<org.apache.parquet.format.FileMetaData> edit) throws IOException {
    java.nio.file.Path local = java.nio.file.Path.of(path.toUri());
    byte[] original = Files.readAllBytes(local);
    int footerLength = BytesUtils.readIntLittleEndian(original, original.length - 8);
    int footerStart = original.length - 8 - footerLength;
    org.apache.parquet.format.FileMetaData footer = readFooter(path);
    edit.accept(footer);
    ByteArrayOutputStream encodedFooter = new ByteArrayOutputStream();
    Util.writeFileMetaData(footer, encodedFooter);
    ByteArrayOutputStream rewritten = new ByteArrayOutputStream();
    rewritten.write(original, 0, footerStart);
    rewritten.write(encodedFooter.toByteArray());
    BytesUtils.writeIntLittleEndian(rewritten, encodedFooter.size());
    rewritten.write(original, original.length - 4, 4);
    Files.write(local, rewritten.toByteArray());
  }

  static void writeSplit(Path path) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message m { required group v (VECTOR(5)) {" + " repeated group list { required float element; } } }");
    ParquetFileWriter writer = new ParquetFileWriter(
        HadoopOutputFile.fromPath(path, new Configuration()),
        schema,
        CREATE,
        DEFAULT_BLOCK_SIZE,
        MAX_PADDING_SIZE_DEFAULT,
        null,
        ParquetProperties.builder().build());
    writer.start();
    writer.startBlock(2);
    writer.startColumn(schema.getColumns().get(0), 10, CompressionCodecName.UNCOMPRESSED);
    int start = 0;
    for (int count : new int[] {3, 4, 3}) {
      boolean packed = start == 3;
      ValuesWriter rl = packed
          ? new ByteBitPackingValuesWriter(1, Packer.BIG_ENDIAN)
          : new RunLengthBitPackingHybridValuesWriter(1, 64, 64, new HeapByteBufferAllocator());
      ValuesWriter dl = packed
          ? new ByteBitPackingValuesWriter(1, Packer.BIG_ENDIAN)
          : new RunLengthBitPackingHybridValuesWriter(1, 64, 64, new HeapByteBufferAllocator());
      ValuesWriter values = new PlainValuesWriter(64, 64, new HeapByteBufferAllocator());
      try {
        for (int i = start; i < start + count; i++) {
          rl.writeInteger(i == 0 || i == 5 ? 0 : 1);
          dl.writeInteger(1);
          values.writeFloat(i);
        }
        BytesInput bytes = BytesInput.concat(
            BytesInput.from(rl.getBytes().toByteArray()),
            BytesInput.from(dl.getBytes().toByteArray()),
            BytesInput.from(values.getBytes().toByteArray()));
        Encoding levelEncoding = packed ? BIT_PACKED : RLE;
        writer.writeDataPage(count, (int) bytes.size(), bytes, levelEncoding, levelEncoding, PLAIN);
      } finally {
        rl.close();
        dl.close();
        values.close();
      }
      start += count;
    }
    writer.endColumn();
    writer.endBlock();
    writer.end(Collections.emptyMap());
    writer.close();
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: VectorInteropFileGenerator <output-directory>");
    }
    java.nio.file.Path dir = java.nio.file.Path.of(args[0]);
    Files.createDirectories(dir);
    writeMixed(new Path(dir.resolve("vector_mixed_v1_plain.parquet").toUri()), WriterVersion.PARQUET_1_0, false);
    writeMixed(
        new Path(dir.resolve("vector_mixed_v2_dictionary.parquet").toUri()), WriterVersion.PARQUET_2_0, true);
    writeSplit(new Path(dir.resolve("vector_split_v1.parquet").toUri()));
    writeInvalid(
        new Path(dir.resolve("vector_invalid_v1_dictionary.parquet").toUri()));
  }
}
