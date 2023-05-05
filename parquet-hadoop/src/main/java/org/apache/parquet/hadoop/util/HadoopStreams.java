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

package org.apache.parquet.hadoop.util;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.util.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Objects;

/**
 * Convenience methods to get Parquet abstractions for Hadoop data streams.
 */
public class HadoopStreams {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStreams.class);

  private static final DynMethods.UnboundMethod hasCapabilitiesMethod =
    new DynMethods
      .Builder("hasCapabilities")
      .impl(FSDataInputStream.class, "hasCapabilities", String.class)
      .orNoop()
      .build();

  /**
   * Wraps a {@link FSDataInputStream} in a {@link SeekableInputStream}
   * implementation for Parquet readers.
   *
   * @param stream a Hadoop FSDataInputStream
   * @return a SeekableInputStream
   */
  public static SeekableInputStream wrap(FSDataInputStream stream) {
    Objects.requireNonNull(stream, "Cannot wrap a null input stream");

    // Try to check using hasCapabilities(str)
    Boolean hasCapabilitiesResult = isWrappedStreamByteBufferReadable(stream);

    // If it is null, then fall back to the old method
    if (hasCapabilitiesResult != null) {
      if (hasCapabilitiesResult) {
        return new H2SeekableInputStream(stream);
      } else {
        return new H1SeekableInputStream(stream);
      }
    }

    return unwrapByteBufferReadableLegacy(stream);
  }

  /**
   * Is the inner stream byte buffer readable?
   * The test is 'the stream is not FSDataInputStream
   * and implements ByteBufferReadable'
   *
   * This logic is only used for Hadoop <2.9.x, and <3.x.x
   *
   * @param stream stream to probe
   * @return A H2SeekableInputStream to access, or H1SeekableInputStream if the stream is not seekable
   */
  private static SeekableInputStream unwrapByteBufferReadableLegacy(FSDataInputStream stream) {
    InputStream wrapped = stream.getWrappedStream();
    if (wrapped instanceof FSDataInputStream) {
      LOG.debug("Checking on wrapped stream {} of {} whether is ByteBufferReadable", wrapped, stream);
      return unwrapByteBufferReadableLegacy(((FSDataInputStream) wrapped));
    }
    if (stream.getWrappedStream() instanceof ByteBufferReadable) {
      return new H2SeekableInputStream(stream);
    } else {
      return new H1SeekableInputStream(stream);
    }
  }

  /**
   * Is the inner stream byte buffer readable?
   * The test is 'the stream is not FSDataInputStream
   * and implements ByteBufferReadable'
   *
   * That is: all streams which implement ByteBufferReadable
   * other than FSDataInputStream successfully support read(ByteBuffer).
   * This is true for all filesystem clients the hadoop codebase.
   *
   * In hadoop 3.3.0+, the StreamCapabilities probe can be used to
   * check this: only those streams which provide the read(ByteBuffer)
   * semantics MAY return true for the probe "in:readbytebuffer";
   * FSDataInputStream will pass the probe down to the underlying stream.
   *
   * @param stream stream to probe
   * @return true if it is safe to a H2SeekableInputStream to access
   * the data, null when it cannot be determined because of missing hasCapabilities
   */
  private static Boolean isWrappedStreamByteBufferReadable(FSDataInputStream stream) {
    if (hasCapabilitiesMethod.isNoop()) {
      // When the method is not available, just return a null
      return null;
    }

    boolean isByteBufferReadable = hasCapabilitiesMethod.invoke(stream, "in:readbytebuffer");

    if (isByteBufferReadable) {
      // stream is issuing the guarantee that it implements the
      // API. Holds for all implementations in hadoop-*
      // since Hadoop 3.3.0 (HDFS-14111).
      return true;
    }
    InputStream wrapped = stream.getWrappedStream();
    if (wrapped instanceof FSDataInputStream) {
      LOG.debug("Checking on wrapped stream {} of {} whether is ByteBufferReadable", wrapped, stream);
      return isWrappedStreamByteBufferReadable(((FSDataInputStream) wrapped));
    }
    return wrapped instanceof ByteBufferReadable;
  }


  /**
   * Wraps a {@link FSDataOutputStream} in a {@link PositionOutputStream}
   * implementation for Parquet writers.
   *
   * @param stream a Hadoop FSDataOutputStream
   * @return a SeekableOutputStream
   */
  public static PositionOutputStream wrap(FSDataOutputStream stream) {
    Objects.requireNonNull(stream, "Cannot wrap a null output stream");
    return new HadoopPositionOutputStream(stream);
  }
}
