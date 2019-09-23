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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ToAvroCommandTest extends AvroFileTest {
  @Test
  public void testToAvroCommand() throws IOException {
    File avroFile = toAvro(parquetFile());
    Assert.assertTrue(avroFile.exists());
  }

  @Test
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

  @Test
  public void testToAvroCommandOverwriteExistentFile() throws IOException {
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avro");
    FileUtils.touch(outputFile);
    Assert.assertEquals(0, outputFile.length());
    File avroFile = toAvro(parquetFile(), outputFile, true);
    Assert.assertTrue(0 < avroFile.length());
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testToAvroCommandOverwriteExistentFileWithoutOverwriteOption() throws IOException {
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avro");
    FileUtils.touch(outputFile);
    toAvro(parquetFile(), outputFile, false);
  }
}
