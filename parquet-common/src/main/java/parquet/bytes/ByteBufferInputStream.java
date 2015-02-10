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
package parquet.bytes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * This ByteBufferInputStream does not consume the ByteBuffer being passed in, 
 * but will create a slice of the current buffer.
 */
public class ByteBufferInputStream extends InputStream {
	
  protected ByteBuffer byteBuf;
  protected int initPos;
  protected int count;
  public ByteBufferInputStream(ByteBuffer buffer) {
    this(buffer, buffer.position(), buffer.remaining());
  }
  
  public ByteBufferInputStream(ByteBuffer buffer, int offset, int count) {
    ByteBuffer temp = buffer.duplicate();
    temp.position(offset);
    byteBuf = temp.slice();
    byteBuf.limit(count);
    this.initPos = offset;
    this.count = count;
  }
  
  public ByteBuffer toByteBuffer() {
    return byteBuf.slice();
  }
  
  @Override
  public int read() throws IOException {
    if (!byteBuf.hasRemaining()) {
    	return -1;
    }
    //Workaround for unsigned byte
    return byteBuf.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    int count = Math.min(byteBuf.remaining(), length);
    if (count == 0) return -1;
    byteBuf.get(bytes, offset, count);
    return count;
  }
  
  @Override
  public long skip(long n) {
	  if (n > byteBuf.remaining())
	    n = byteBuf.remaining();
	  int pos = byteBuf.position();
	  byteBuf.position((int)(pos + n));
	  return n;
  }


  @Override
  public int available() {
    return byteBuf.remaining();
  }
}
