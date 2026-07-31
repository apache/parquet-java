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

import static org.apache.parquet.hadoop.ParquetInputFormat.READ_SUPPORT_CLASS;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.recordlevel.PhoneBookWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests of a footer, and the file it was read from, being passed on to a reader through
 * {@link ParquetInputSplit#setFooter(ParquetMetadata)} rather than being read again.
 */
@SuppressWarnings("deprecation")
public class TestFooterReuse {

  private static final List<PhoneBookWriter.User> DATA = TestParquetReader.makeUsers(100);

  /** Size of the footer length and the trailing magic: only a footer read goes here. */
  private static final int TAIL_SIZE = ParquetFileWriter.MAGIC.length + Integer.BYTES;

  @TempDir
  static java.nio.file.Path tempDir;

  private static Path file;
  private static long fileLength;

  @BeforeAll
  public static void writeFile() throws IOException {
    file = new Path(tempDir.resolve("phonebook.parquet").toString());
    PhoneBookWriter.write(ExampleParquetWriter.builder(file), DATA);
    fileLength = file.getFileSystem(new Configuration()).getFileStatus(file).getLen();
  }

  @Test
  public void testFooterKnowsItsInputFile() throws Exception {
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      assertThat(reader.getFooter().getInputFile()).isSameAs(inputFile);
    }
  }

  @Test
  public void testInputFileNotSerializedToJson() throws Exception {
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      footer = reader.getFooter();
    }

    assertThat(footer.getInputFile()).isNotNull();
    assertThat(ParquetMetadata.toJSON(footer)).doesNotContain("inputFile");
  }

  /**
   * A split carrying a footer is read without going back to the file for it: the record reader returns the same
   * records as one which reads the footer itself, without ever touching the tail of the file where the footer
   * length and the magic live.
   */
  @Test
  public void testSplitFooterIsReused() throws Exception {
    CountingInputFile footerReadFile = new CountingInputFile(HadoopInputFile.fromPath(file, new Configuration()));
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(footerReadFile)) {
      footer = reader.getFooter();
    }
    assertThat(footerReadFile.tailBytesRead())
        .as("bytes read from the tail of the file while reading the footer")
        .isPositive();

    CountingInputFile countingFile = new CountingInputFile(HadoopInputFile.fromPath(file, new Configuration()));
    List<String> baseline = readSplit(newSplit(file), null, null);
    List<String> reusing = readSplit(newSplit(file), countingFile, footer);

    assertThat(reusing).hasSize(DATA.size()).isEqualTo(baseline);
    assertThat(countingFile.bytesRead())
        .as("bytes read through the file supplied with the footer")
        .isPositive();
    assertThat(countingFile.tailBytesRead())
        .as("bytes read from the last %d bytes of the file, which only a footer read touches", TAIL_SIZE)
        .isZero();
  }

  /**
   * The file recorded in the footer is the one opened, so a split whose own path no longer resolves is still
   * readable when it carries a footer.
   */
  @Test
  public void testFooterInputFileIsUsedOverSplitPath() throws Exception {
    InputFile inputFile = HadoopInputFile.fromPath(file, new Configuration());
    ParquetMetadata footer;
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      footer = reader.getFooter();
    }

    Path missing = new Path(file.getParent(), "no-such-file.parquet");
    List<String> records = readSplit(newSplit(missing), inputFile, footer);

    assertThat(records).hasSize(DATA.size());
  }

  /**
   * A split carrying a footer serializes to json without it: the footer is state of the JVM which built the split,
   * not part of its description.
   */
  @Test
  public void testFooterNotSerializedToJson() throws Exception {
    ParquetInputSplit split = newSplit(file);
    split.setFooter(readFooter());

    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    String json = mapper.writeValueAsString(split);

    assertThat(json).contains("rowGroupOffsets").doesNotContain("footer");
  }

  /**
   * A split carrying a footer survives a Writable round trip with everything else intact, but the footer
   * itself does not cross it: a split deserialized in a task has to read the footer as it always did.
   */
  @Test
  public void testFooterNotSerializedThroughWritable() throws Exception {
    ParquetInputSplit split = newSplit(file);
    split.setFooter(readFooter());

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      split.write(out);
    }
    ParquetInputSplit read = new ParquetInputSplit();
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      read.readFields(in);
    }

    assertThat(read.getFooter())
        .as("footer of a split read back from its serialized form")
        .isNull();
    assertThat(read.getPath()).isEqualTo(split.getPath());
    assertThat(read.getStart()).isEqualTo(split.getStart());
    assertThat(read.getEnd()).isEqualTo(split.getEnd());
    assertThat(read.getLength()).isEqualTo(split.getLength());
  }

  private static ParquetMetadata readFooter() throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, new Configuration()))) {
      return reader.getFooter();
    }
  }

  private static ParquetInputSplit newSplit(Path path) {
    return new ParquetInputSplit(path, 0, fileLength, fileLength, new String[0], null);
  }

  /**
   * Read a split through a {@link ParquetRecordReader}, with the footer and the file it was read from attached to
   * the split when a footer is supplied.
   */
  private static List<String> readSplit(ParquetInputSplit split, InputFile inputFile, ParquetMetadata footer)
      throws IOException, InterruptedException {
    if (footer != null) {
      footer.setInputFile(inputFile);
      split.setFooter(footer);
    }

    Configuration conf = new Configuration();
    conf.set(READ_SUPPORT_CLASS, GroupReadSupport.class.getName());
    TaskAttemptContext taskContext =
        ContextUtil.newTaskAttemptContext(conf, TaskAttemptID.forName("attempt_0_1_m_1_1"));

    List<String> records = new ArrayList<>();
    ParquetRecordReader<Group> reader = new ParquetRecordReader<>(new GroupReadSupport());
    try {
      reader.initialize(split, taskContext);
      while (reader.nextKeyValue()) {
        records.add(reader.getCurrentValue().toString());
      }
    } finally {
      reader.close();
    }
    return records;
  }

  /**
   * An {@link InputFile} which counts the bytes read through the streams it hands out, separately counting those
   * read from the tail of the file: the footer length and the trailing magic.
   */
  private static final class CountingInputFile implements InputFile {

    private final InputFile wrapped;
    private final long tailStart;
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong tailBytesRead = new AtomicLong();

    public long bytesRead() {
      return bytesRead.get();
    }

    public long tailBytesRead() {
      return tailBytesRead.get();
    }

    private CountingInputFile(InputFile wrapped) throws IOException {
      this.wrapped = wrapped;
      this.tailStart = wrapped.getLength() - TAIL_SIZE;
    }

    @Override
    public long getLength() throws IOException {
      return wrapped.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return new CountingInputStream(wrapped.newStream());
    }

    private final class CountingInputStream extends SeekableInputStream {

      private final SeekableInputStream in;

      private CountingInputStream(SeekableInputStream in) {
        this.in = in;
      }

      /**
       * Count a read of {@code read} bytes which started at {@code start}.
       */
      private int counted(long start, int read) {
        if (read > 0) {
          bytesRead.addAndGet(read);
          tailBytesRead.addAndGet(Math.max(0, start + read - Math.max(start, tailStart)));
        }
        return read;
      }

      @Override
      public long getPos() throws IOException {
        return in.getPos();
      }

      @Override
      public void seek(long newPos) throws IOException {
        in.seek(newPos);
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        long start = in.getPos();
        in.readFully(bytes);
        counted(start, bytes.length);
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        long pos = in.getPos();
        in.readFully(bytes, start, len);
        counted(pos, len);
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        long start = in.getPos();
        return counted(start, in.read(buf));
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        long start = in.getPos();
        int remaining = buf.remaining();
        in.readFully(buf);
        counted(start, remaining);
      }

      @Override
      public int read() throws IOException {
        long start = in.getPos();
        int read = in.read();
        if (read >= 0) {
          counted(start, 1);
        }
        return read;
      }

      @Override
      public int read(byte[] bytes, int off, int len) throws IOException {
        long start = in.getPos();
        return counted(start, in.read(bytes, off, len));
      }

      @Override
      public void close() throws IOException {
        in.close();
      }
    }
  }
}
