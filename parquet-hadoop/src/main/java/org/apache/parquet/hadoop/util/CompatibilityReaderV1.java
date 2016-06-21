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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Uses Hadoop V1's readFully(byte[], ...) APIs to read data.
 */
public class CompatibilityReaderV1 implements CompatibilityReader {

  @Override
  public int readBuf(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int res;
    if (readBuf.hasArray()) {
      res = readWithExistingArray(f, readBuf);
    } else {
      res = readWithNewArray(f, readBuf);
    }

    return res;
  }

  private int readWithExistingArray(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int initPos = readBuf.position();
    int remaining = readBuf.remaining();
    f.readFully(readBuf.array(), readBuf.arrayOffset(), remaining);
    readBuf.position(initPos + remaining);
    return remaining;
  }

  private int readWithNewArray(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int remaining = readBuf.remaining();
    byte[] buf = new byte[remaining];
    f.readFully(buf);
    readBuf.put(buf, 0, remaining);
    return remaining;
  }
}
