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

package org.apache.parquet.hadoop.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Class which implements {@link #readFully(ByteBuffer)} through
 * {@code ByteBufferPositionedReadable.readFully()}.
 * <p>This is implemented by HDFS and possibly other clients,
 */
class H3ByteBufferInputStream extends H2SeekableInputStream {
  public H3ByteBufferInputStream(final FSDataInputStream stream) {
    super(stream);
  }

  @Override
  public FSDataInputStream getStream() {
    return (FSDataInputStream) super.getStream();
  }

  /**
   * Read the buffer fully through use of {@code ByteBufferPositionedReadable.readFully()}
   * at the current location.
   * <p>That operation is designed to not use the current reading position, rather
   * an absolute position is passed in.
   * In the use here the original read position is saved, and
   * after the read is finished a {@code seek()} call made to move the
   * cursor on.
   *
   * @param buf a byte buffer to fill with data from the stream
   *
   * @throws EOFException the buffer length is greater than the file length
   * @throws IOException other IO problems.
   */
  @Override
  public void readFully(final ByteBuffer buf) throws EOFException, IOException {
    // use ByteBufferPositionedReadable.readFully()
    final FSDataInputStream stream = getStream();
    // remember the current position
    final long pos = stream.getPos();
    final int size = buf.remaining();
    stream.readFully(pos, buf);
    // then move read position on afterwards.
    stream.seek(pos + size);
  }
}
