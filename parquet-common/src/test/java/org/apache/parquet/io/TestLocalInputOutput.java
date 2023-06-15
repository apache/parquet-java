/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.io;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TestLocalInputOutput {

  Path pathFileExists = Paths.get("src/test/resources/disk_output_file_create_overwrite.parquet");
  Path pathNewFile = Paths.get("src/test/resources/disk_output_file_create.parquet");

  @Test
  public void outputFileOverwritesFile() throws IOException {
    OutputFile write = new LocalOutputFile(pathFileExists);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(124);
    }
    InputFile read = new LocalInputFile(pathFileExists);
    try (SeekableInputStream stream = read.newStream()) {
      assertEquals(stream.read(), 124);
      assertEquals(stream.read(), -1);
    }
  }

  @Test
  public void outputFileCreateFailsAsFileAlreadyExists() {
    OutputFile write = new LocalOutputFile(pathFileExists);
    assertThrows(FileAlreadyExistsException.class, () -> write.create(512));
  }

  @Test
  public void outputFileCreatesFileWithOverwrite() throws IOException {
    OutputFile write = new LocalOutputFile(pathNewFile);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(255);
    }
    InputFile read = new LocalInputFile(pathNewFile);
    try (SeekableInputStream stream = read.newStream()) {
      assertEquals(stream.read(), 255);
      assertEquals(stream.read(), -1);
    }
    Files.delete(pathNewFile);
  }

  @Test
  public void outputFileCreatesFile() throws IOException {
    OutputFile write = new LocalOutputFile(pathNewFile);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(2);
    }
    InputFile read = new LocalInputFile(pathNewFile);
    try (SeekableInputStream stream = read.newStream()) {
      assertEquals(stream.read(), 2);
      assertEquals(stream.read(), -1);
    }
    Files.delete(pathNewFile);
  }
}
