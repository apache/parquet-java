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

import com.beust.jcommander.JCommander;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ToAvroCommandTest extends AvroFileTest {
  @TempDir
  private Path tempDir;

  @Test
  public void testToAvroCommandFromParquet() throws IOException {
    File avroFile = toAvro(parquetFile());
    assertThat(avroFile).exists();
  }

  @Test
  public void testToAvroCommandFromJson() throws IOException {
    final File jsonInputFile = tempDir.resolve("sample.json").toFile();
    Files.createFile(jsonInputFile.toPath());
    final File avroOutputFile = tempDir.resolve("sample.avro").toFile();
    Files.createFile(avroOutputFile.toPath());

    // Write the json to the file, so we can read it again.
    final String inputJson = "{\"id\": 1, \"name\": \"Alice\"}\n" + "{\"id\": 2, \"name\": \"Bob\"}\n"
        + "{\"id\": 3, \"name\": \"Carol\"}\n"
        + "{\"id\": 4, \"name\": \"Dave\"}";

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(jsonInputFile))) {
      writer.write(inputJson);
    }

    ToAvroCommand cmd = new ToAvroCommand(null);

    JCommander.newBuilder()
        .addObject(cmd)
        .build()
        .parse("--overwrite", jsonInputFile.getAbsolutePath(), "--output", avroOutputFile.getAbsolutePath());

    assertThat(cmd.run()).isZero();
  }

  @Test
  public void testToAvroCommandWithGzipCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "GZIP");
    assertThat(avroFile).exists();
  }

  @Test
  public void testToAvroCommandWithSnappyCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "SNAPPY");
    assertThat(avroFile).exists();
  }

  @Test
  public void testToAvroCommandWithZstdCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "ZSTD");
    assertThat(avroFile).exists();
  }

  @Test
  public void testToAvroCommandWithBzip2Compression() throws IOException {
    File avroFile = toAvro(parquetFile(), "bzip2");
    assertThat(avroFile).exists();
  }

  @Test
  public void testToAvroCommandWithXzCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "xz");
    assertThat(avroFile).exists();
  }

  @Test
  public void testToAvroCommandWithInvalidCompression() throws IOException {
    File parquetFile = parquetFile();
    assertThatThrownBy(() -> toAvro(parquetFile, "FOO"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Codec incompatible with Avro: FOO");
  }

  @Test
  public void testToAvroCommandOverwriteExistentFile() throws IOException {
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avro");
    FileUtils.touch(outputFile);
    assertThat(outputFile.length()).isZero();
    File avroFile = toAvro(parquetFile(), outputFile, true);
    assertThat(avroFile.length()).isPositive();
  }

  @Test
  public void testToAvroCommandOverwriteExistentFileWithoutOverwriteOption() throws IOException {
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avro");
    FileUtils.touch(outputFile);
    File parquetFile = parquetFile();
    assertThatThrownBy(() -> toAvro(parquetFile, outputFile, false))
        .isInstanceOf(FileAlreadyExistsException.class)
        .hasMessageContaining("File already exists");
  }
}
