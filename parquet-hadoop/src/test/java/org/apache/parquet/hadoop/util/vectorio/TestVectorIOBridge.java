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

package org.apache.parquet.hadoop.util.vectorio;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntFunction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.io.ParquetFileRange;

import static org.apache.parquet.hadoop.util.vectorio.BindingUtils.awaitFuture;
import static org.apache.parquet.hadoop.util.vectorio.VectorIOBridge.VECTOREDIO_CAPABILITY;
import static org.apache.parquet.hadoop.util.vectorio.VectorIOBridge.instance;
import static org.apache.parquet.hadoop.util.vectorio.VectorIOBridge.readVectoredRanges;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Test the vector IO bridge.
 * Much of this is lifted from hadoop-common test
 * {@code AbstractContractVectoredReadTest};
 * with other utility methods from
 * {@code org.apache.hadoop.fs.contract.ContractTestUtils}.
 */
public class TestVectorIOBridge {
  private static final int DATASET_LEN = 64 * 1024;
  private static final byte[] DATASET = dataset(DATASET_LEN, 'a', 32);
  private static final String VECTORED_READ_FILE_NAME = "vectored_file.txt";

  /**
   * Timeout in seconds for vectored read operation in tests : {@value}.
   */
  private static final int VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS = 5 * 60;

  /**
   * relative vectored path.
   */
  private final Path vectoredPath = new Path("target/test/vectored");

  private final HeapByteBufferAllocator bufferAllocator = new HeapByteBufferAllocator();
  private final ElasticByteBufferPool pool =
    new ElasticByteBufferPool();

  private final IntFunction<ByteBuffer> allocate = value -> {
    return pool.getBuffer(false, value);
  };

  private FileSystem fileSystem;
  private Path testFilePath;

  public TestVectorIOBridge() {

  }

  @Before
  public void setUp() throws IOException {
    // skip the tests if the FileRangeBridge goes not load.
    assumeTrue("Bridge not available", FileRangeBridge.bridgeAvailable());

    fileSystem = FileSystem.getLocal(new Configuration());
    testFilePath = fileSystem.makeQualified(vectoredPath);
    createFile(fileSystem, testFilePath, DATASET);
  }

  @After
  public void tearDown() throws IOException {
    if (fileSystem != null) {
      fileSystem.delete(testFilePath, false);
    }
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  /**
   * If the file range bridge is available, so must be the vector io bridge.
   */
  @Test
  public void testVectorIOBridgeAvailable() throws Throwable {
    assertTrue("VectorIOBridge not available", VectorIOBridge.bridgeAvailable());
  }

  /**
   * Create a dataset for use in the tests; all data is in the range
   * base to (base+modulo-1) inclusive.
   *
   * @param len length of data
   * @param base base of the data
   * @param modulo the modulo
   *
   * @return the newly generated dataset
   */
  private static byte[] dataset(int len, int base, int modulo) {
    byte[] dataset = new byte[len];
    for (int i = 0; i < len; i++) {
      dataset[i] = (byte) (base + (i % modulo));
    }
    return dataset;
  }

  /**
   * Utility to return buffers back to the pool once all
   * data has been read for each file range.
   *
   * @param fileRanges list of file range.
   * @param pool buffer pool.
   *
   * @throws IOException any IOE
   * @throws TimeoutException ideally this should never occur.
   */
  public static void returnBuffersToPoolPostRead(List<ParquetFileRange> fileRanges,
    ByteBufferPool pool)
    throws IOException, TimeoutException {
    for (ParquetFileRange range : fileRanges) {
      ByteBuffer buffer = awaitFuture(range.getDataReadFuture(),
        VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
        TimeUnit.SECONDS);
      pool.putBuffer(buffer);
    }
  }

  /**
   * Create a file.
   *
   * @param fs filesystem
   * @param path path to write
   * @param data source dataset. Can be null
   *
   * @throws IOException on any problem
   */
  public static void createFile(FileSystem fs,
    Path path,
    byte[] data) throws IOException {
    try (FSDataOutputStream stream = fs.create(path, true)) {
      if (data != null && data.length > 0) {
        stream.write(data);
      }
    }
  }

  /**
   * Open the test file.
   * @return test file input stream
   * @throws IOException failure to open
   */
  private FSDataInputStream openTestFile() throws IOException {
    return getFileSystem().open(testFilePath);
  }

  /**
   * Read a list of ranges, all adjacent.
   */
  @Test
  public void testVectoredReadMultipleRanges() throws Exception {
    FileSystem fs = getFileSystem();
    List<ParquetFileRange> fileRanges = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      fileRanges.add(range(i * 100, 100));
    }
    try (FSDataInputStream in = fs.open(testFilePath)) {
      readVectoredRanges(in, fileRanges, allocate);
      CompletableFuture<?>[] completableFutures = new CompletableFuture<?>[fileRanges.size()];
      int i = 0;
      for (ParquetFileRange res : fileRanges) {
        completableFutures[i++] = res.getDataReadFuture();
      }
      CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(completableFutures);
      combinedFuture.get();

      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * VectorIO and readFully() can coexist.
   */
  @Test
  public void testVectoredReadAndReadFully() throws Exception {
    final int offset = 100;
    final int length = 256;
    List<ParquetFileRange> fileRanges = ranges(offset, length);

    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      byte[] readFullRes = new byte[length];
      in.readFully(offset, readFullRes);
      ByteBuffer vecRes = awaitFuture(fileRanges.get(0).getDataReadFuture(),
        VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      final byte[] array = vecRes.array();
      assertDatasetEquals(0, "readFully",
        vecRes, length, readFullRes);
    } finally {
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024,none of the test ranges
   * will get merged.
   */
  @Test
  public void testDisjointRanges() throws Exception {
    List<ParquetFileRange> fileRanges = ranges(
      0, 100,
      4_000 + 101, 100,
      16_000 + 101, 100);

    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * Verify the stream really implements the readVectored API, rather
   * than fall back to the base implementation.
   */
  @Test
  public void testStreamImplementsReadVectored() throws Exception {

    try (FSDataInputStream in = openTestFile()) {
      final boolean streamDoesNativeVectorIO = instance().hasCapability(
        in,
        VECTOREDIO_CAPABILITY);
      assertTrue("capability " + VECTOREDIO_CAPABILITY + " not supported by " + in,
        streamDoesNativeVectorIO);
    }
  }

  /**
   * As the minimum seek value is 4*1024, all the below ranges
   * will get merged into one.
   */
  @Test
  public void testAllRangesMergedIntoOne() throws Exception {
    List<ParquetFileRange> fileRanges = ranges(
      0, 100,
      4_000 + 101, 100,
      16_000 + 101, 100);
    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * As the minimum seek value is 4*1024, the first three ranges will be
   * merged into and other two will remain as it is.
   */
  @Test
  public void testSomeRangesMergedSomeUnmerged() throws Exception {
    List<ParquetFileRange> fileRanges = ranges(
      8 * 1024, 100,
      14 * 1024, 100,
      10 * 1024, 100,
      2 * 1024 - 101, 100,
      40 * 1024, 1024);
    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testOverlappingRanges() throws Exception {
    List<ParquetFileRange> fileRanges = getSampleOverlappingRanges();
    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testSameRanges() throws Exception {
    // Same ranges are special case of overlapping only.
    List<ParquetFileRange> fileRanges = getSampleSameRanges();

    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testSomeRandomNonOverlappingRanges() throws Exception {
    List<ParquetFileRange> fileRanges = ranges(
      500, 100,
      1000, 200,
      50, 10,
      10, 5);
    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testConsecutiveRanges() throws Exception {
    List<ParquetFileRange> fileRanges = ranges(
      500, 100,
      600, 200,
      800, 100);
    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testNegativeLengthRange() throws Exception {
    verifyExceptionalVectoredRead(getFileSystem(), ranges(1, -50),
      IllegalArgumentException.class);
  }

  /**
   * Negative ranges are rejected; the inner cause is an
   * {@code EOFException}.
   */
  @Test
  public void testNegativeOffsetRange() throws Exception {
    final RuntimeException ex = verifyExceptionalVectoredRead(
      getFileSystem(), ranges(-1, 50),
      RuntimeException.class);
    if (!(ex.getCause() instanceof EOFException)) {
      throw ex;
    }
  }

  /**
   * Classic seek/read read after vectored IO.
   */
  @Test
  public void testNormalReadAfterVectoredRead() throws Exception {
    List<ParquetFileRange> fileRanges = getSampleNonOverlappingRanges();
    try (FSDataInputStream in = openTestFile()) {
      readVectoredRanges(in, fileRanges, allocate);

      // read starting 200 bytes
      byte[] res = new byte[200];
      in.read(res, 0, 200);
      ByteBuffer buffer = ByteBuffer.wrap(res);
      assertDatasetEquals(0, "normal_read", buffer, 200, DATASET);
      assertEquals("Vectored read shouldn't change file pointer.", 200, in.getPos());
      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  /**
   * Vectored IO after Classic seek/read.
   */
  @Test
  public void testVectoredReadAfterNormalRead() throws Exception {
    List<ParquetFileRange> fileRanges = getSampleNonOverlappingRanges();
    try (FSDataInputStream in = openTestFile()) {
      // read starting 200 bytes
      byte[] res = new byte[200];
      in.read(res, 0, 200);
      ByteBuffer buffer = ByteBuffer.wrap(res);
      assertDatasetEquals(0, "normal_read", buffer, 200, DATASET);
      assertEquals("Vectored read shouldn't change file pointer.", 200, in.getPos());
      readVectoredRanges(in, fileRanges, allocate);

      validateVectoredReadResult(fileRanges, DATASET);
      returnBuffersToPoolPostRead(fileRanges, pool);
    }
  }

  @Test
  public void testMultipleVectoredReads() throws Exception {
    List<ParquetFileRange> fileRanges1 = getSampleNonOverlappingRanges();

    List<ParquetFileRange> fileRanges2 = getSampleNonOverlappingRanges();
    try (FSDataInputStream in = openTestFile()) {

      readVectoredRanges(in, fileRanges1, allocate);
      readVectoredRanges(in, fileRanges2, allocate);

      validateVectoredReadResult(fileRanges2, DATASET);
      validateVectoredReadResult(fileRanges1, DATASET);
      returnBuffersToPoolPostRead(fileRanges1, pool);
      returnBuffersToPoolPostRead(fileRanges2, pool);
    }
  }

  /**
   * Test to validate EOF ranges. Default implementation fails with EOFException
   * while reading the ranges. Some implementation like s3, checksum fs fail fast
   * as they already have the file length calculated.
   *
   * @throws Exception
   */
/*
@Test
public void testEOFRanges()  throws Exception {
  FileSystem fs = getFileSystem();
  List<ParquetFileRange> fileRanges = ranges(DATASET_LEN, 100);
  try (FSDataInputStream in = fs.open(testFilePath)) {
    in.readVectored(fileRanges, allocate);
    readVectoredRanges(in, fileRanges, allocate);
    for (FileRange res : fileRanges) {
      CompletableFuture<ByteBuffer> data = res.getData();
      interceptFuture(EOFException.class,
              "",
              VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
              TimeUnit.SECONDS,
              data);
    }
  }
}*/

  /**
   * Create a ParquetFileRange instance.
   * @param offset offset in file
   * @param length range length
   * @return a range
   */
  private ParquetFileRange range(final long offset, final int length) {
    return new ParquetFileRange(offset, length);
  }

  /**
   * Create a list of ranges where the arguments are expected to
   * be pairs of int offset and range.
   * @param args an even-numbered list.
   * @return a list of ranges.
   */
  private List<ParquetFileRange> ranges(int... args) {
    final int len = args.length;
    assertTrue("range argument length of " + len + " is not even",
      (len % 1) == 0);
    List<ParquetFileRange> fileRanges = new ArrayList<>();
    for (int i = 0; i < len; i += 2) {
      fileRanges.add(range(args[i], args[i + 1]));
    }
    return fileRanges;
  }

  protected List<ParquetFileRange> getSampleNonOverlappingRanges() {
    return ranges(0, 100, 110, 50);
  }

  protected List<ParquetFileRange> getSampleOverlappingRanges() {
    return ranges(
      100, 500,
      400, 500);
  }

  protected List<ParquetFileRange> getConsecutiveRanges() {
    return ranges(
      100, 500,
      600, 500);
  }

  protected List<ParquetFileRange> getSampleSameRanges() {
    return ranges(
      8_000, 1000,
      8_000, 1000,
      8_000, 1000);
  }

  /**
   * Assert that the data read matches the dataset at the given offset.
   * This helps verify that the seek process is moving the read pointer
   * to the correct location in the file.
   *
   * @param readOffset the offset in the file where the read began.
   * @param operation operation name for the assertion.
   * @param data data read in.
   * @param length length of data to check.
   * @param originalData original data.
   */
  public static void assertDatasetEquals(
    final int readOffset,
    final String operation,
    final ByteBuffer data,
    int length, byte[] originalData) {
    for (int i = 0; i < length; i++) {
      int o = readOffset + i;
      assertEquals(operation + " with read offset " + readOffset
          + ": data[" + i + "] != DATASET[" + o + "]",
        originalData[o], data.get());
    }
  }

  /**
   * Utility to validate vectored read results.
   *
   * @param fileRanges input ranges.
   * @param originalData original data.
   *
   * @throws IOException any ioe.
   */
  public static void validateVectoredReadResult(List<ParquetFileRange> fileRanges,
    byte[] originalData)
    throws IOException, TimeoutException {
    CompletableFuture<?>[] completableFutures = new CompletableFuture<?>[fileRanges.size()];
    int i = 0;
    for (ParquetFileRange res : fileRanges) {
      completableFutures[i++] = res.getDataReadFuture();
    }
    CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(completableFutures);
    awaitFuture(combinedFuture,
      VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
      TimeUnit.SECONDS);

    for (ParquetFileRange res : fileRanges) {
      CompletableFuture<ByteBuffer> data = res.getDataReadFuture();
      ByteBuffer buffer = awaitFuture(data,
        VECTORED_READ_OPERATION_TEST_TIMEOUT_SECONDS,
        TimeUnit.SECONDS);
      assertDatasetEquals((int) res.getOffset(), "vecRead",
        buffer, res.getLength(), originalData);
    }
  }

  /**
   * Validate that exceptions must be thrown during a vectored
   * read operation with specific input ranges.
   *
   * @param fs FileSystem instance.
   * @param fileRanges input file ranges.
   * @param clazz type of exception expected.
   *
   * @throws Exception any other IOE.
   */
  protected <T extends Throwable> T verifyExceptionalVectoredRead(
    FileSystem fs,
    List<ParquetFileRange> fileRanges,
    Class<T> clazz) throws Exception {

    try (FSDataInputStream in = fs.open(testFilePath)) {
      readVectoredRanges(in, fileRanges, allocate);
      fail("expected error reading " + in);
      // for the compiler
      return null;
    } catch (AssertionError e) {
      throw e;
    } catch (Exception e) {
      if (!clazz.isAssignableFrom(e.getClass())) {
        throw e;
      }
      return (T) e;
    }
  }
}
