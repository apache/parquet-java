/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import parquet.format.FileMetaData;
import parquet.org.apache.thrift.ShortStack;
import parquet.org.apache.thrift.TBase;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.*;
import parquet.org.apache.thrift.transport.TTransport;
import parquet.org.apache.thrift.transport.TTransportException;

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.EnumSet;
import java.lang.reflect.Method;

public class CompatibilityUtil {
  private static final ByteBufferPool bufferPool = new ElasticByteBufferPool();
  private static final EnumSet<ReadOption> ZCOPY_OPTS =
      EnumSet.of(ReadOption.SKIP_CHECKSUMS);
  private static final int MAX_SIZE = 1 << 20;
  
  public static int getInt(FSDataInputStream f) throws IOException {
    ByteBuffer int32Buf = getBuf(f, 4).order(ByteOrder.LITTLE_ENDIAN);
    if (int32Buf.remaining() == 4) {
      final int res = int32Buf.getInt();
      f.releaseBuffer(int32Buf);
      return res;
    }
    ByteBuffer tmpBuf = int32Buf;
    int32Buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    int32Buf.put(tmpBuf);
    f.releaseBuffer(tmpBuf);
    while (int32Buf.hasRemaining()) {
      tmpBuf = getBuf(f, int32Buf.remaining());
      int32Buf.put(tmpBuf);
      f.releaseBuffer(tmpBuf);
    }
    return int32Buf.getInt();
  }
  
  public static ByteBuffer getBuf(FSDataInputStream f, int maxSize)
      throws IOException {
    Class<?>[] ZCopyArgs = {ByteBufferPool.class, int.class, EnumSet.class};
    ByteBuffer res = null;
    try {
      //Try to Get Zero Copy API
      f.getClass().getMethod("read", ZCopyArgs);
      res = f.read(bufferPool, maxSize, ZCOPY_OPTS);
    } catch (NoSuchMethodException e) {
      byte[] buf = new byte[maxSize];
      f.readFully(buf);
      res = ByteBuffer.wrap(buf);
    }
    
    if (res == null) {
      throw new EOFException("Null ByteBuffer returned");
    }
    return res;
  }
}