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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.jupiter.api.Test;

public class RewriteCommandTest extends ParquetFileTest {
  @Test
  public void testRewriteCommand() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted.parquet");
    command.output = output.getAbsolutePath();
    command.setConf(new Configuration());
    assertThat(command.run()).isZero();
    assertThat(output).exists();
  }

  @Test
  public void testRewriteCommandWithoutOverwrite() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted.parquet");
    command.output = output.getAbsolutePath();
    command.setConf(new Configuration());

    Files.createFile(output.toPath());
    assertThatThrownBy(command::run)
        .isInstanceOf(FileAlreadyExistsException.class)
        .hasMessageContaining("File already exists");
  }

  @Test
  public void testRewriteCommandWithOverwrite() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted.parquet");
    command.output = output.getAbsolutePath();
    command.overwrite = true;
    command.setConf(new Configuration());

    Files.createFile(output.toPath());
    assertThat(command.run()).isZero();
    assertThat(output).exists();
  }

  @Test
  public void testRewriteCommandWithCompression_GZIP() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted-1.GZIP.parquet");
    command.output = output.getAbsolutePath();
    command.codec = "GZIP";
    command.setConf(new Configuration());

    assertThat(command.run()).isZero();
    assertThat(output).exists();
  }

  @Test
  public void testRewriteCommandWithCompression_gzip() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted-2.gzip.parquet");
    command.output = output.getAbsolutePath();
    command.codec = "gzip";
    command.setConf(new Configuration());

    assertThat(command.run()).isZero();
    assertThat(output).exists();
  }

  @Test
  public void testRewriteCommandWithPruneColumnsUpdatesAvroSchema() throws IOException {
    Configuration conf = new Configuration();
    Schema schema = SchemaBuilder.record("people")
        .fields()
        .optionalLong("id")
        .optionalString("name")
        .requiredString("ssn")
        .endRecord();
    File input = new File(getTempFolder(), "people.parquet");
    try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
            HadoopOutputFile.fromPath(new Path(input.getAbsolutePath()), conf))
        .withSchema(schema)
        .withConf(conf)
        .build()) {
      GenericRecord alice = new GenericData.Record(schema);
      alice.put("id", 1L);
      alice.put("name", "Alice");
      alice.put("ssn", "111-22-3333");
      writer.write(alice);
      GenericRecord bob = new GenericData.Record(schema);
      bob.put("id", 2L);
      bob.put("name", "Bob");
      bob.put("ssn", "444-55-6666");
      writer.write(bob);
    }

    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Collections.singletonList(input.getAbsolutePath());
    File output = new File(getTempFolder(), "pruned.parquet");
    command.output = output.getAbsolutePath();
    command.pruneColumns = Collections.singletonList("ssn");
    command.setConf(conf);
    assertThat(command.run()).isZero();

    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
            HadoopInputFile.fromPath(new Path(output.getAbsolutePath()), conf))
        .withConf(conf)
        .build()) {
      for (GenericRecord record = reader.read(); record != null; record = reader.read()) {
        records.add(record);
      }
    }
    assertThat(records).hasSize(2);
    assertThat(records.get(0).getSchema().getField("ssn")).isNull();
    assertThat(records.get(0).getSchema().getFields()).hasSize(2);
    assertThat(records.get(0).get("id")).isEqualTo(1L);
    assertThat(records.get(1).get("name")).hasToString("Bob");
  }
}
