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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.PositionOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Convenience methods to get Parquet abstractions for Hadoop data streams.
 */
public class HadoopStreams {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStreams.class);

  private static final Class<?> byteBufferReadableClass = getReadableClass();
  static final Constructor<SeekableInputStream> h2SeekableConstructor = getH2SeekableConstructor();

  /**
   * Wraps a {@link FSDataInputStream} in a {@link SeekableInputStream}
   * implementation for Parquet readers.
   *
   * @param stream a Hadoop FSDataInputStream
   * @return a SeekableInputStream
   */
  public static SeekableInputStream wrap(FSDataInputStream stream) {
    Preconditions.checkNotNull(stream, "Cannot wrap a null input stream");
    if (byteBufferReadableClass != null && h2SeekableConstructor != null &&
        byteBufferReadableClass.isInstance(stream.getWrappedStream())) {
      try {
        return h2SeekableConstructor.newInstance(stream);
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.warn("Could not instantiate H2SeekableInputStream, falling back to byte array reads", e);
        return new H1SeekableInputStream(stream);
      } catch (InvocationTargetException e) {
        throw new ParquetDecodingException(
            "Could not instantiate H2SeekableInputStream", e.getTargetException());
      }
    } else {
      return new H1SeekableInputStream(stream);
    }
  }

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
   * Wraps a {@link FSDataOutputStream} in a {@link PositionOutputStream}
   * implementation for Parquet writers.
   *
   * @param stream a Hadoop FSDataOutputStream
   * @return a SeekableOutputStream
   */
  public static PositionOutputStream wrap(FSDataOutputStream stream) {
    Preconditions.checkNotNull(stream, "Cannot wrap a null output stream");
    return new HadoopPositionOutputStream(stream);
  }
}
