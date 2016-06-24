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
 * Uses the Hadoop V2 read(ByteBuffer) APIs to read data.
 */
public class CompatibilityReaderV2 implements CompatibilityReader {

  @Override
  public int readFully(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int remaining = readBuf.remaining();
    while (readBuf.hasRemaining()) {
      int readCount = f.read(readBuf);
      if (readCount == -1) {
        // this is probably a bug in the ParquetReader. We shouldn't have called readFully with a buffer
        // that has more remaining than the amount of data in the stream.
        throw new EOFException("Reached the end of stream. Still have: " + readBuf.remaining() + " bytes left");
      }
    }

    return remaining;
  }
}
