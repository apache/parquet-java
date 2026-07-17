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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
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
      assertThat(stream.read()).isEqualTo(124);
      assertThat(stream.read()).isEqualTo(-1);
    }
  }

  @Test
  public void outputFileCreateFailsAsFileAlreadyExists() throws IOException {
    Path path = Paths.get(createTempFile().getPath());
    OutputFile write = new LocalOutputFile(path);
    write.create(512).close();
    assertThatThrownBy(() -> write.create(512).close())
        .isInstanceOf(FileAlreadyExistsException.class)
        .hasMessage(path.toString());
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
      assertThat(stream.read()).isEqualTo(255);
      assertThat(stream.read()).isEqualTo(-1);
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
      assertThat(stream.read()).isEqualTo(2);
      assertThat(stream.read()).isEqualTo(-1);
    }
  }

  private File createTempFile() throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return tmp;
  }

  @Test
  public void readFullyIntoHeapByteBuffer() throws IOException {
    Path path = writeBytes(new byte[] {1, 2, 3, 4, 5});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(5);
      stream.readFully(buf);
      assertThat(buf.position()).isEqualTo(5);
      buf.flip();
      byte[] out = new byte[5];
      buf.get(out);
      assertThat(out).isEqualTo(new byte[] {1, 2, 3, 4, 5});
    }
  }

  @Test
  public void readFullyIntoHeapByteBufferWithNonZeroPosition() throws IOException {
    Path path = writeBytes(new byte[] {10, 20, 30, 40});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(6);
      buf.put(new byte[] {99, 99}); // advance position to 2
      stream.readFully(buf);
      assertThat(buf.position()).isEqualTo(6);
      buf.flip();
      byte[] out = new byte[6];
      buf.get(out);
      assertThat(out).isEqualTo(new byte[] {99, 99, 10, 20, 30, 40});
    }
  }

  @Test
  public void readFullyIntoDirectByteBuffer() throws IOException {
    Path path = writeBytes(new byte[] {7, 8, 9});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocateDirect(3);
      stream.readFully(buf);
      assertThat(buf.position()).isEqualTo(3);
      buf.flip();
      byte[] out = new byte[3];
      buf.get(out);
      assertThat(out).isEqualTo(new byte[] {7, 8, 9});
    }
  }

  @Test
  public void readFullyIntoReadOnlyByteBuffer() throws IOException {
    Path path = writeBytes(new byte[] {7, 8, 9});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer backing = ByteBuffer.allocate(3);
      ByteBuffer buf = backing.asReadOnlyBuffer();
      assertThatThrownBy(() -> stream.readFully(buf)).isInstanceOf(ReadOnlyBufferException.class);
    }
  }

  @Test
  public void readIntoHeapByteBuffer() throws IOException {
    Path path = writeBytes(new byte[] {1, 2, 3, 4});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      int read = stream.read(buf);
      assertThat(read).isEqualTo(4);
      assertThat(buf.position()).isEqualTo(4);
      buf.flip();
      byte[] out = new byte[4];
      buf.get(out);
      assertThat(out).isEqualTo(new byte[] {1, 2, 3, 4});
    }
  }

  @Test
  public void readIntoByteBufferAdvancesPositionByBytesRead() throws IOException {
    Path path = writeBytes(new byte[] {1, 2, 3});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(10);
      int read = stream.read(buf);
      assertThat(read).isEqualTo(3);
      assertThat(buf.position()).isEqualTo(3);
    }
  }

  @Test
  public void readIntoByteBufferReturnsMinusOneAtEof() throws IOException {
    Path path = writeBytes(new byte[] {1});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      assertThat(stream.read()).isEqualTo(1);
      ByteBuffer buf = ByteBuffer.allocate(4);
      int read = stream.read(buf);
      assertThat(read).isEqualTo(-1);
      assertThat(buf.position()).isEqualTo(0);
    }
  }

  @Test
  public void readIntoDirectByteBuffer() throws IOException {
    Path path = writeBytes(new byte[] {7, 8, 9});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocateDirect(3);
      int read = stream.read(buf);
      assertThat(read).isEqualTo(3);
      assertThat(buf.position()).isEqualTo(3);
      buf.flip();
      byte[] out = new byte[3];
      buf.get(out);
      assertThat(out).isEqualTo(new byte[] {7, 8, 9});
    }
  }

  @Test
  public void readIntoByteBufferWithNonZeroPosition() throws IOException {
    Path path = writeBytes(new byte[] {10, 20, 30});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(5);
      buf.put(new byte[] {99, 99}); // advance position to 2
      int read = stream.read(buf);
      assertThat(read).isEqualTo(3);
      assertThat(buf.position()).isEqualTo(5);
      buf.flip();
      byte[] out = new byte[5];
      buf.get(out);
      assertThat(out).isEqualTo(new byte[] {99, 99, 10, 20, 30});
    }
  }

  @Test
  public void readFullyThrowsEofWhenStreamTooShort() throws IOException {
    Path path = writeBytes(new byte[] {1, 2});
    try (SeekableInputStream stream = new LocalInputFile(path).newStream()) {
      ByteBuffer buf = ByteBuffer.allocate(10);
      assertThatThrownBy(() -> stream.readFully(buf)).isInstanceOf(EOFException.class);
    }
  }

  private Path writeBytes(byte[] data) throws IOException {
    Path path = Paths.get(createTempFile().getPath());
    OutputFile write = new LocalOutputFile(path);
    try (PositionOutputStream stream = write.createOrOverwrite(512)) {
      stream.write(data);
    }
    return path;
  }
}
