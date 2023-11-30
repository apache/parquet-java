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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

public class TestLocalInputOutput {

  @Test
  public void outputFileOverwritesFile() throws IOException {
    Path path = Paths.get(createTempFile().getPath());
    OutputFile write = new LocalOutputFile(path);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(124);
    }
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(124);
    }
    InputFile read = new LocalInputFile(path);
    try (SeekableInputStream stream = read.newStream()) {
      assertEquals(stream.read(), 124);
      assertEquals(stream.read(), -1);
    }
  }

  @Test
  public void outputFileCreateFailsAsFileAlreadyExists() throws IOException {
    Path path = Paths.get(createTempFile().getPath());
    OutputFile write = new LocalOutputFile(path);
    write.create(512).close();
    assertThrows(FileAlreadyExistsException.class, () -> write.create(512).close());
  }

  @Test
  public void outputFileCreatesFileWithOverwrite() throws IOException {
    Path path = Paths.get(createTempFile().getPath());
    OutputFile write = new LocalOutputFile(path);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(255);
    }
    InputFile read = new LocalInputFile(path);
    try (SeekableInputStream stream = read.newStream()) {
      assertEquals(stream.read(), 255);
      assertEquals(stream.read(), -1);
    }
  }

  @Test
  public void outputFileCreatesFile() throws IOException {
    Path path = Paths.get(createTempFile().getPath());
    OutputFile write = new LocalOutputFile(path);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(2);
    }
    InputFile read = new LocalInputFile(path);
    try (SeekableInputStream stream = read.newStream()) {
      assertEquals(stream.read(), 2);
      assertEquals(stream.read(), -1);
    }
  }

  private File createTempFile() throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return tmp;
  }
}
