/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util;

import static org.apache.hadoop.fs.FileSystemTestBinder.addFileSystemForTesting;
import static org.apache.hadoop.fs.Options.OpenFileOptions.FS_OPTION_OPENFILE_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestBinder;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.impl.FutureDataInputStreamBuilderImpl;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Test suite to validate behavior of opening files through
 * use of FileSystem.openFile(), especially fallback
 * to the original FileSystem.open() method when
 * openFile() raises an IllegalArgumentException or other RTE.
 * <p>
 * These tests use classes in the package org.apache.hadoop.fs.impl}.
 * Although an implementation package, it is tagged as `LimitedPrivate("Filesystems")`;
 * it is already used outside the hadoop codebase (e.g. google gcs).
 */
public class TestHadoopOpenFile {

  private static final int FIRST = MockHadoopInputStream.TEST_ARRAY[0];

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private URI fsUri;
  private FileStatus status;
  private Path path;
  private Configuration conf;
  private IOException fileNotFound;
  private IllegalArgumentException illegal;

  @Before
  public void setUp() throws URISyntaxException {
    // the schema "mock:" is used to not only be confident our injected
    // instance is picked up, but to ensure that there will be no
    // contamination of any real schema, such as file:
    fsUri = new URI("mock://path/");
    path = new Path("mock://path/file");

    conf = new Configuration(false);
    fileNotFound = new FileNotFoundException("file not found");
    illegal = new IllegalArgumentException("illegal");
    status = new FileStatus(10, false, 1, 1, 0, path);
  }

  /**
   * Clean up the entire FS cache for the current user.
   */
  @After
  public void tearDown() {
    FileSystemTestBinder.cleanFilesystemCache();
  }

  /**
   * The healthy path for opening a file.
   */
  @Test
  public void testOpenFileGoodPath() throws Throwable {
    final FileSystem mockFS = prepareMockFS();
    final FSDataInputStream in = new FSDataInputStream(new MockHadoopInputStream());
    final StubOpenFileBuilder opener = new StubOpenFileBuilder(mockFS, path, CompletableFuture.completedFuture(in));
    doReturn(opener).when(mockFS).openFile(path);

    // this looks up the FS binding via the status file path.
    openAndRead(fileFromStatus());

    assertThat(opener.getFileStatus()).as("file status").isSameAs(status);

    // The fallback call of open(path) never took place.
    Mockito.verify(mockFS, never()).open(path);
  }

  /**
   * A caller-supplied length avoids a status lookup and is passed to openFile().
   */
  @Test
  public void testOpenFileWithKnownLength() throws Throwable {
    final FileSystem mockFS = prepareMockFS();
    final FSDataInputStream in = new FSDataInputStream(new MockHadoopInputStream());
    final StubOpenFileBuilder opener = new StubOpenFileBuilder(mockFS, path, CompletableFuture.completedFuture(in));
    doReturn(opener).when(mockFS).openFile(path);

    final HadoopInputFile inputFile = HadoopInputFile.fromPath(path, status.getLen(), conf);
    assertThat(inputFile.getPath()).as("path").isEqualTo(path);
    assertThat(inputFile.getLength()).as("length").isEqualTo(status.getLen());
    openAndRead(inputFile);

    Mockito.verify(mockFS, never()).getFileStatus(path);
    assertThat(opener.getFileStatus()).as("file status").isNull();
    assertThat(opener.getOptions().get(FS_OPTION_OPENFILE_LENGTH))
        .as("openFile length")
        .isEqualTo(Long.toString(status.getLen()));
  }

  @Test
  public void testRejectsNegativeKnownLength() {
    assertThatThrownBy(() -> HadoopInputFile.fromPath(path, -1, conf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file length: -1");
  }

  /**
   * Opening a nonexistent file with a caller-supplied length must fail either
   * when opening the stream or on the first read.
   */
  @Test
  public void testKnownLengthMissingFile() throws IOException {
    final Path missingPath = new Path(new Path(temp.getRoot().toURI()), "missing.parquet");
    final HadoopInputFile inputFile = HadoopInputFile.fromPath(missingPath, 1, conf);

    assertThatThrownBy(() -> {
          try (SeekableInputStream stream = inputFile.newStream()) {
            stream.read();
          }
        })
        .isInstanceOf(FileNotFoundException.class);
  }

  /**
   * The openFile() call raises a RuntimeException which it is caught and the
   * classic open() call invoked.
   */
  @Test
  public void testOpenFileEarlyFailure() throws Throwable {
    final FileSystem mockFS = prepareMockFS();
    final FSDataInputStream in = new FSDataInputStream(new MockHadoopInputStream());

    Mockito.doThrow(illegal).when(mockFS).openFile(path);
    doReturn(in).when(mockFS).open(path);

    // this looks up the FS binding via the status file path.
    openAndRead(fileFromStatus());
  }

  /**
   * openFile() failure during the completable future execution with an
   * RTE raised.
   * Again, this triggers a fallback to open().
   */
  @Test
  public void testOpenFileLateFailure() throws Throwable {
    final FileSystem mockFS = prepareMockFS();
    final FSDataInputStream in = new FSDataInputStream(new MockHadoopInputStream());

    final StubOpenFileBuilder opener = new StubOpenFileBuilder(
        mockFS, path, CompletableFuture.completedFuture(null).thenApply((f) -> {
          throw illegal;
        }));
    doReturn(opener).when(mockFS).openFile(path);
    doReturn(in).when(mockFS).open(path);

    openAndRead(fileFromStatus());
  }

  /**
   * Open a stream, read the first byte, and assert that it matches
   * what is expected.
   *
   * @param inputFile input file
   *
   * @throws IOException failure to open
   */
  private static void openAndRead(final HadoopInputFile inputFile) throws IOException {
    try (SeekableInputStream stream = inputFile.newStream()) {
      assertThat(stream.read()).as("byte read").isEqualTo(FIRST);
    }
  }

  /**
   * If openFile() raises an IOException within the future,
   * then it is thrown and the classic open() call never invoked.
   */
  @Test
  public void testOpenFileRaisesIOException() throws Throwable {
    final FileSystem mockFS = prepareMockFS();

    final StubOpenFileBuilder opener = new StubOpenFileBuilder(
        mockFS, path, CompletableFuture.completedFuture(null).thenApply((f) -> {
          // throw a wrapped IOE
          throw new UncheckedIOException(fileNotFound);
        }));
    doReturn(opener).when(mockFS).openFile(path);

    final HadoopInputFile inputFile = fileFromStatus();
    assertThatThrownBy(inputFile::newStream)
        .isInstanceOf(FileNotFoundException.class)
        .hasMessage("file not found");
    Mockito.verify(mockFS, never()).open(path);
  }

  /**
   * If openFile() raises a RuntimeException, this it is caught and the.
   * classic open() call invoked.
   * If that call raises an IOE.
   * Outcome: the IOE is thrown but the caught RTE is added to the
   * suppressed list.
   */
  @Test
  public void testOpenFileDoubleFailure() throws Throwable {
    final FileSystem mockFS = prepareMockFS();

    Mockito.doThrow(illegal).when(mockFS).openFile(path);
    Mockito.doThrow(fileNotFound).when(mockFS).open(path);

    // this looks up the FS binding via the status file path.
    final HadoopInputFile inputFile = fileFromStatus();

    assertThatThrownBy(inputFile::newStream)
        .isInstanceOf(FileNotFoundException.class)
        .isSameAs(fileNotFound)
        .hasSuppressedException(illegal);
  }

  /**
   * The handling of a double RTE is the same as the case of
   * the sequence of: RTE, IOE.
   */
  @Test
  public void testOpenFileDoubleRTE() throws Throwable {
    final FileSystem mockFS = prepareMockFS();

    Mockito.doThrow(illegal).when(mockFS).openFile(path);
    NullPointerException npe = new NullPointerException("null");
    Mockito.doThrow(npe).when(mockFS).open(path);

    // this looks up the FS binding via the status file path.
    final HadoopInputFile inputFile = fileFromStatus();

    assertThatThrownBy(inputFile::newStream)
        .isInstanceOf(NullPointerException.class)
        .isSameAs(npe)
        .hasSuppressedException(illegal);
  }

  /**
   * Create a mock FileSystem with the foundational operations
   * mocked. The FS is added as the binding for the mock URI.
   *
   * @return a mock FileSystem
   *
   * @throws IOException stub signature only.
   */
  private FileSystem prepareMockFS() throws IOException {
    final FileSystem mockFS = mock(FileSystem.class);
    doNothing().when(mockFS).close();
    doReturn(conf).when(mockFS).getConf();
    doReturn(status).when(mockFS).getFileStatus(path);

    // register the FS instance under the mock URI
    addFileSystemForTesting(fsUri, mockFS);
    return mockFS;
  }

  /**
   * Build an input file from the status field.
   * @return an input file.
   * @throws IOException failure to create the associated filesystem.
   */
  private HadoopInputFile fileFromStatus() throws IOException {
    return HadoopInputFile.fromStatus(status, conf);
  }

  /**
   * Stub implementation of {@link FutureDataInputStreamBuilder}.
   * Trying to mock the interface is troublesome as the interface has added
   * some new methods over time, instead this uses the base implementation class
   * within o.a.h.fs.impl.
   */
  private static final class StubOpenFileBuilder extends FutureDataInputStreamBuilderImpl {

    /**
     * Operation to invoke to build the result.
     */
    private final CompletableFuture<FSDataInputStream> result;

    /**
     * Create the builder. The FS and path must be non-null.
     *
     * @param fileSystem fs
     * @param path path to open
     * @param result builder result.
     */
    private StubOpenFileBuilder(
        final FileSystem fileSystem, Path path, final CompletableFuture<FSDataInputStream> result) {
      super(fileSystem, path);
      this.result = result;
    }

    private FileStatus getFileStatus() {
      return getStatus();
    }

    @Override
    public CompletableFuture<FSDataInputStream> build()
        throws IllegalArgumentException, UnsupportedOperationException {
      return result;
    }
  }
}
