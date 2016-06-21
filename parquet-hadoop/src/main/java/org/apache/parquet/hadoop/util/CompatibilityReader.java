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
package org.apache.parquet.hadoop.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Allows us to use either Hadoop V1 / V2 read APIs to read data without reflection. HadoopV2 implementation
 * of this interface resides in a module with Hadoop2 dependencies.
 * Note: classes that implement this interface are instantiated using reflection and must thus have a
 * default constructor.
 */
public interface CompatibilityReader {

  /**
   * This method attempts to read into the provided readBuffer, readBuffer.remaining() bytes.
   * @return Number of bytes read - should be readBuf.remaining()
   * @throws EOFException if readBuf.remaining() is greater than the number of bytes available to
   * read on the FSDataInputStream f.
   */
  int readBuf(FSDataInputStream f, ByteBuffer readBuf) throws IOException;
}
