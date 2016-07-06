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
import java.io.IOException;
import java.nio.ByteBuffer;

public class H2MockInputStream extends MockInputStream implements ByteBufferReadable {
  public H2MockInputStream(int... actualReadLengths) {
    super(actualReadLengths);
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    // this is inefficient, but simple for correctness tests of
    // readFully(ByteBuffer)
    byte[] temp = new byte[buf.remaining()];
    int bytesRead = read(temp, 0, temp.length);
    if (bytesRead > 0) {
      buf.put(temp, 0, bytesRead);
    }
    return bytesRead;
  }
}
