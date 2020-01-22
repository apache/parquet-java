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

import com.beust.jcommander.JCommander;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ToAvroCommandTest extends AvroFileTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testToAvroCommandFromParquet() throws IOException {
    File avroFile = toAvro(parquetFile());
    Assert.assertTrue(avroFile.exists());
  }

  @Test
  public void testToAvroCommandFromJson() throws IOException {
    final File jsonInputFile = folder.newFile("sample.json");
    final File avroOutputFile = folder.newFile("sample.avro");

    // Write the json to the file, so we can read it again.
    final String inputJson = "{\"id\": 1, \"name\": \"Alice\"}\n" + "{\"id\": 2, \"name\": \"Bob\"}\n"
        + "{\"id\": 3, \"name\": \"Carol\"}\n" + "{\"id\": 4, \"name\": \"Dave\"}";

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(jsonInputFile))) {
      writer.write(inputJson);
    }

    ToAvroCommand cmd = new ToAvroCommand(null);

    JCommander.newBuilder().addObject(cmd).build().parse(jsonInputFile.getAbsolutePath(), "--output",
        avroOutputFile.getAbsolutePath());

    assert (cmd.run() == 0);
  }

  public void testToAvroCommandWithGzipCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "GZIP");
    Assert.assertTrue(avroFile.exists());
  }

  @Test
  public void testToAvroCommandWithSnappyCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "SNAPPY");
    Assert.assertTrue(avroFile.exists());
  }

  @Test
  public void testToAvroCommandWithZstdCompression() throws IOException {
    File avroFile = toAvro(parquetFile(), "ZSTD");
    Assert.assertTrue(avroFile.exists());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToAvroCommandWithInvalidCompression() throws IOException {
    toAvro(parquetFile(), "FOO");
  }
}
