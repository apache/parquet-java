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

import static org.apache.parquet.hadoop.util.vectorio.BindingUtils.loadInvocation;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.util.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vector IO bridge.
 * <p>
 * This loads the {@code PositionedReadable} method:
 * <pre>
 *   void readVectored(List[?extends FileRange] ranges,
 *     IntFunction[ByteBuffer] allocate) throws IOException
 * </pre>
 * It is made slightly easier because of type erasure; the signature of the
 * function is actually {@code Void.class method(List.class, IntFunction.class)}.
 * <p>
 * There are some counters to aid in testing; the {@link #toString()} method
 * will print them and the loaded method, for use in tests and debug logs.
 */
public final class VectorIOBridge {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIOBridge.class);

  /**
   * readVectored Method to look for.
   */
  private static final String READ_VECTORED = "readVectored";

  /**
   * hasCapability Method name.
   * {@code boolean hasCapability(String capability);}
   */
  private static final String HAS_CAPABILITY = "hasCapability";

  /**
   * hasCapability() probe for vectored IO api implementation.
   */
  static final String VECTOREDIO_CAPABILITY = "in:readvectored";

  /**
   * The singleton instance of the bridge.
   */
  private static final VectorIOBridge INSTANCE = new VectorIOBridge();

  /**
   * readVectored() method.
   */
  private final DynMethods.UnboundMethod readVectored;

  /**
   * {@code boolean StreamCapabilities.hasCapability(String)}.
   */
  private final DynMethods.UnboundMethod hasCapabilityMethod;

  /**
   * How many vector read calls made.
   */
  private final AtomicLong vectorReads = new AtomicLong();

  /**
   * How many blocks were read.
   */
  private final AtomicLong blocksRead = new AtomicLong();

  /**
   * How many bytes were read.
   */
  private final AtomicLong bytesRead = new AtomicLong();

  /**
   * Constructor. package private for testing.
   */
  private VectorIOBridge() {

    readVectored =
        loadInvocation(PositionedReadable.class, Void.TYPE, READ_VECTORED, List.class, IntFunction.class);
    LOG.debug("Vector IO availability; ", available());

    // if readVectored is present, so is hasCapabilities().
    hasCapabilityMethod = loadInvocation(FSDataInputStream.class, boolean.class, HAS_CAPABILITY, String.class);
  }

  /**
   * Is the bridge available.
   *
   * @return true if readVectored() is available.
   */
  public boolean available() {
    return !readVectored.isNoop() && FileRangeBridge.bridgeAvailable();
  }

  /**
   * Check that the bridge is available.
   *
   * @throws UnsupportedOperationException if it is not.
   */
  private void checkAvailable() {
    if (!available()) {
      throw new UnsupportedOperationException("Hadoop VectorIO not found");
    }
  }

  /**
   * Read data in a list of file ranges.
   * Returns when the data reads are active; possibly executed
   * in a blocking sequence of reads, possibly scheduled on different
   * threads.
   * The {@link ParquetFileRange} parameters all have their
   * data read futures set to the range reads of the associated
   * operations; callers must await these to complete.
   *
   * @param stream stream from where the data has to be read.
   * @param ranges parquet file ranges.
   * @param allocate allocate function to allocate memory to hold data.
   * @throws UnsupportedOperationException if the API is not available.
   */
  public static void readVectoredRanges(
      final FSDataInputStream stream,
      final List<ParquetFileRange> ranges,
      final IntFunction<ByteBuffer> allocate) {

    final VectorIOBridge bridge = availableInstance();
    final FileRangeBridge rangeBridge = FileRangeBridge.instance();
    // Setting the parquet range as a reference.
    List<FileRangeBridge.WrappedFileRange> fileRanges =
        ranges.stream().map(rangeBridge::toFileRange).collect(Collectors.toList());
    bridge.readWrappedRanges(stream, fileRanges, allocate);

    // copy back the completable futures from the scheduled
    // vector reads to the ParquetFileRange entries passed in.
    fileRanges.forEach(fileRange -> {
      // toFileRange() sets up this back reference
      ParquetFileRange parquetFileRange = (ParquetFileRange) fileRange.getReference();
      parquetFileRange.setDataReadFuture(fileRange.getData());
    });
  }

  /**
   * Read data in a list of wrapped file ranges, extracting the inner
   * instances and then executing.
   * Returns when the data reads are active; possibly executed
   * in a blocking sequence of reads, possibly scheduled on different
   * threads.
   *
   * @param stream stream from where the data has to be read.
   * @param ranges wrapped file ranges.
   * @param allocate allocate function to allocate memory to hold data.
   */
  private void readWrappedRanges(
      final PositionedReadable stream,
      final List<FileRangeBridge.WrappedFileRange> ranges,
      final IntFunction<ByteBuffer> allocate) {

    // update the counters.
    vectorReads.incrementAndGet();
    blocksRead.addAndGet(ranges.size());
    // extract the instances the wrapped ranges refer to; update the
    // bytes read counter.
    List<Object> instances = ranges.stream()
        .map(r -> {
          bytesRead.addAndGet(r.getLength());
          return r.getFileRange();
        })
        .collect(Collectors.toList());
    LOG.debug("readVectored with {} ranges on stream {}", ranges.size(), stream);
    readVectored.invoke(stream, instances, allocate);
  }

  @Override
  public String toString() {
    return "VectorIOBridge{"
        + "readVectored=" + readVectored
        + ", vectorReads=" + vectorReads.get()
        + ", blocksRead=" + blocksRead.get()
        + ", bytesRead=" + bytesRead.get()
        + '}';
  }

  /**
   * Does a stream implement StreamCapability and, if so, is the capability
   * available.
   * If the method is not found, this predicate will return false.
   * @param stream input stream to query.
   * @param capability the capability to look for.
   * @return true if the stream declares the capability is available.
   */
  public boolean hasCapability(final FSDataInputStream stream, final String capability) {

    if (hasCapabilityMethod.isNoop()) {
      return false;
    } else {
      return hasCapabilityMethod.invoke(stream, capability);
    }
  }

  /**
   * How many vector read calls have been made?
   * @return the count of vector reads.
   */
  public long getVectorReads() {
    return vectorReads.get();
  }

  /**
   * How many blocks were read?
   * @return the count of blocks read.
   */
  public long getBlocksRead() {
    return blocksRead.get();
  }

  /**
   * How many bytes of data have been read.
   * @return the count of bytes read.
   */
  public long getBytesRead() {
    return bytesRead.get();
  }

  /**
   * Reset all counters; for testing.
   * Non-atomic.
   */
  void resetCounters() {
    vectorReads.set(0);
    blocksRead.set(0);
    bytesRead.set(0);
  }

  /**
   * Get the singleton instance.
   *
   * @return instance.
   */
  public static VectorIOBridge instance() {
    return INSTANCE;
  }

  /**
   * Is the bridge available for use?
   *
   * @return true if the vector IO API is available.
   */
  public static boolean bridgeAvailable() {
    return instance().available();
  }

  /**
   * Get the instance <i>verifying that the API is available</i>.
   * @return an available instance, always
   * @throws UnsupportedOperationException if it is not.
   */
  public static VectorIOBridge availableInstance() {
    final VectorIOBridge bridge = instance();
    bridge.checkAvailable();
    return bridge;
  }
}
