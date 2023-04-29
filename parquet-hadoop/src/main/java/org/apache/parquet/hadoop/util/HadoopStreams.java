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
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Objects;

/**
 * Convenience methods to get Parquet abstractions for Hadoop data streams.
 */
public class HadoopStreams {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStreams.class);

  private static final Class<?> byteBufferReadableClass = getReadableClass();
  static final Constructor<SeekableInputStream> h2SeekableConstructor = getH2SeekableConstructor();

  private static Class<?> getReadableClass() {
    try {
      return Class.forName("org.apache.hadoop.fs.ByteBufferReadable");
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private static Class<SeekableInputStream> getH2SeekableClass() {
    try {
      return (Class<SeekableInputStream>) Class.forName(
        "org.apache.parquet.hadoop.util.H2SeekableInputStream");
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      return null;
    }
  }

  private static Constructor<SeekableInputStream> getH2SeekableConstructor() {
    Class<SeekableInputStream> h2SeekableClass = getH2SeekableClass();
    if (h2SeekableClass != null) {
      try {
        return h2SeekableClass.getConstructor(FSDataInputStream.class);
      } catch (NoSuchMethodException e) {
        return null;
      }
    }
    return null;
  }

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
    Boolean hasCapabilitiesResult = isWrappedStreamByteBufferReadableHasCapabilities(stream);

    // If it is null, then fall back to the old method
    if (hasCapabilitiesResult != null) {
      if (hasCapabilitiesResult) {
        return new H2SeekableInputStream(stream);
      } else {
        return new H1SeekableInputStream(stream);
      }
    }

    return isWrappedStreamByteBufferReadableLegacy(stream);
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
  private static SeekableInputStream isWrappedStreamByteBufferReadableLegacy(FSDataInputStream stream) {
    InputStream wrapped = stream.getWrappedStream();
    if (wrapped instanceof FSDataInputStream) {
      LOG.debug("Checking on wrapped stream {} of {} whether is ByteBufferReadable", wrapped, stream);
      return isWrappedStreamByteBufferReadableLegacy(((FSDataInputStream) wrapped));
    }
    if (byteBufferReadableClass != null && h2SeekableConstructor != null &&
      byteBufferReadableClass.isInstance(stream.getWrappedStream())) {
      try {
        return h2SeekableConstructor.newInstance(stream);
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.warn("Could not instantiate H2SeekableInputStream, falling back to byte array reads", e);
      } catch (InvocationTargetException e) {
        throw new ParquetDecodingException(
          "Could not instantiate H2SeekableInputStream", e.getTargetException());
      }
    }
    return new H1SeekableInputStream(stream);
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
   *         the data, null when it cannot be determined
   */
  private static Boolean isWrappedStreamByteBufferReadableHasCapabilities(FSDataInputStream stream) {
    Method methodHasCapabilities;
    try {
      methodHasCapabilities = stream.getClass().getMethod("hasCapability", String.class);
    } catch (Exception e) {
      return null;
    }
    try {
      if ((Boolean) methodHasCapabilities.invoke(stream, "in:readbytebuffer")) {
        // stream is issuing the guarantee that it implements the
        // API. Holds for all implementations in hadoop-*
        // since Hadoop 3.3.0 (HDFS-14111).
        return true;
      }
    } catch (IllegalAccessException | InvocationTargetException e) {
      return null;
    }
    InputStream wrapped = stream.getWrappedStream();
    if (wrapped instanceof FSDataInputStream) {
      LOG.debug("Checking on wrapped stream {} of {} whether is ByteBufferReadable", wrapped, stream);
      return isWrappedStreamByteBufferReadableHasCapabilities(((FSDataInputStream) wrapped));
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
