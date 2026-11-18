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

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestParquetRewriterRenameColumns {
  @TempDir
  private java.nio.file.Path tempDir;

  private final Configuration conf = new Configuration();

  @ParameterizedTest(name = "recompress={0}")
  @ValueSource(booleans = {false, true})
  public void renamingPreservesSchemaStatisticsAndValues(boolean recompress) throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message event {"
        + " optional fixed_len_byte_array(4) amount (DECIMAL(8,2)) = 1;"
        + " optional group tags (LIST) = 2 {"
        + "   repeated group list { optional binary element (STRING) = 3; }"
        + " }"
        + " optional float score columnorder(TYPE_DEFINED_ORDER) = 4;"
        + "}");
    MessageType expected = MessageTypeParser.parseMessageType("message event {"
        + " optional fixed_len_byte_array(4) renamed_amount (DECIMAL(8,2)) = 1;"
        + " optional group renamed_tags (LIST) = 2 {"
        + "   repeated group list { optional binary element (STRING) = 3; }"
        + " }"
        + " optional float renamed_score columnorder(TYPE_DEFINED_ORDER) = 4;"
        + "}");
    Path input = new Path(tempDir.resolve("input.parquet").toUri());
    Path output = new Path(tempDir.resolve("output.parquet").toUri());
    Binary negative = Binary.fromConstantByteArray(new byte[] {-1, -1, -1, -1});
    Binary positive = Binary.fromConstantByteArray(new byte[] {0, 0, 0, 1});
    SimpleGroupFactory groups = new SimpleGroupFactory(schema);
    Group first = groups.newGroup().append("amount", negative).append("score", 1.5f);
    first.addGroup("tags").addGroup("list").append("element", "one");
    Group second = groups.newGroup().append("amount", positive).append("score", 2.5f);
    second.addGroup("tags").addGroup("list").append("element", "two");
    // Separate pages make recompression merge statistics for negative and positive decimals.
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(input)
        .withConf(conf)
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
        .withPageRowCountLimit(1)
        .withMinRowCountForPageSizeCheck(1)
        .withMaxRowCountForPageSizeCheck(1)
        .build()) {
      writer.write(first);
      writer.write(second);
    }

    RewriteOptions.Builder builder = new RewriteOptions.Builder(conf, input, output)
        .renameColumns(Map.of(
            "amount", "renamed_amount",
            "tags", "renamed_tags",
            "score", "renamed_score"));
    if (recompress) {
      builder.transform(CompressionCodecName.GZIP);
    }
    try (ParquetRewriter rewriter = new ParquetRewriter(builder.build())) {
      rewriter.processBlocks();
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(output, conf))) {
      assertThat(reader.getFooter().getFileMetaData().getSchema()).isEqualTo(expected);
      var columns = reader.getFooter().getBlocks().get(0).getColumns();
      assertThat(columns)
          .extracting(column -> column.getCodec())
          .containsOnly(recompress ? CompressionCodecName.GZIP : CompressionCodecName.UNCOMPRESSED);
      var amountStatistics = columns.get(0).getStatistics();
      assertThat(amountStatistics.genericGetMin()).isEqualTo(negative);
      assertThat(amountStatistics.genericGetMax()).isEqualTo(positive);
    }
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), output)
        .withConf(conf)
        .build()) {
      Group actual = reader.read();
      assertThat(actual.getBinary("renamed_amount", 0)).isEqualTo(negative);
      assertThat(actual.getGroup("renamed_tags", 0).getGroup("list", 0).getString("element", 0))
          .isEqualTo("one");
      assertThat(actual.getFloat("renamed_score", 0)).isEqualTo(1.5f);
      actual = reader.read();
      assertThat(actual.getBinary("renamed_amount", 0)).isEqualTo(positive);
      assertThat(actual.getGroup("renamed_tags", 0).getGroup("list", 0).getString("element", 0))
          .isEqualTo("two");
      assertThat(actual.getFloat("renamed_score", 0)).isEqualTo(2.5f);
      assertThat(reader.read()).isNull();
    }
  }
}
