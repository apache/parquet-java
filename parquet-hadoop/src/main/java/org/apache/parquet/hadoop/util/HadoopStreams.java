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
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Convenience methods to get Parquet abstractions for Hadoop data streams.
 */
public class HadoopStreams {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStreams.class);

  /**
   * Wraps a {@link FSDataInputStream} in a {@link SeekableInputStream}
   * implementation for Parquet readers.
   *
   * @param stream a Hadoop FSDataInputStream
   * @return a SeekableInputStream
   */
  public static SeekableInputStream wrap(FSDataInputStream stream) {
    Objects.requireNonNull(stream, "Cannot wrap a null input stream");
    return new H2SeekableInputStream(stream);
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
