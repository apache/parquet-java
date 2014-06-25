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
package parquet.hadoop.util;

import org.apache.hadoop.fs.FSDataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import parquet.Log;

public class CompatibilityUtil {
  private static final boolean useV21;
  
  private static final Log LOG = Log.getLog(CompatibilityUtil.class);
  private static final Constructor<?> ELASTIC_BYTE_BUFFER_CONSTRUCTOR;
  private static final Class<?> ElasticByteBufferCls;
  private static final Class<?> ByteBufferCls;
  private static final Class<? extends Enum> ReadOptionCls;
  private static final Method READ_METHOD;
  private static final Method RELEASE_BUFFER_METHOD;

  
  static {
    boolean v21 = true;
    final String PACKAGE = "org.apache.hadoop";
    Class<?> FSDataInputStreamCls;
    try {
      Class.forName(PACKAGE + ".io.ElasticByteBufferPool");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }
    
    useV21 = v21;
    try {
      if (v21) {
        ElasticByteBufferCls = Class.forName(PACKAGE + ".io.ElasticByteBufferPool");
        ELASTIC_BYTE_BUFFER_CONSTRUCTOR = ElasticByteBufferCls.getConstructor();
        ByteBufferCls = Class.forName(PACKAGE + ".io.ByteBufferPool");
        FSDataInputStreamCls = Class.forName(PACKAGE + ".fs.FSDataInputStream");
        ReadOptionCls = (Class<Enum>)Class.forName(PACKAGE + ".fs.ReadOption");
        READ_METHOD = FSDataInputStreamCls.getMethod("read", ByteBufferCls, int.class, EnumSet.class);
        RELEASE_BUFFER_METHOD = FSDataInputStreamCls.getMethod("releaseBuffer", ByteBuffer.class);
      } else {
        ELASTIC_BYTE_BUFFER_CONSTRUCTOR = null;
        ElasticByteBufferCls = null;
        ByteBufferCls = null;
        ReadOptionCls = null;
        READ_METHOD = null;
        RELEASE_BUFFER_METHOD = null;
      }
    } catch (ClassNotFoundException e) {
      LOG.info("can't find class" + e.toString());
      throw new IllegalArgumentException("Can't find class", e);
    } catch (NoSuchMethodException e) {
      LOG.info("can't find class" + e.toString());
      throw new IllegalArgumentException("Can't find constructor ", e);
    }
  }
  
  public static void releaseBuffer(FSDataInputStream f, ByteBuffer buf) {
    if (useV21) {
      try {
        RELEASE_BUFFER_METHOD.invoke(f, buf);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException("Can't call method", e);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Can't call method", e);
      } catch (InvocationTargetException e) {
        throw new IllegalArgumentException("Can't call method", e);
      }
     } 
  }
  
  public static int getInt(FSDataInputStream f) throws IOException {
    ByteBuffer int32Buf = getBuf(f, 4).order(ByteOrder.LITTLE_ENDIAN);
    if (int32Buf.remaining() == 4) {
      final int res = int32Buf.getInt();
      releaseBuffer(f, int32Buf);
      return res;
    }
    ByteBuffer tmpBuf = int32Buf;
    int32Buf = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    int32Buf.put(tmpBuf);
    releaseBuffer(f, tmpBuf);
    while (int32Buf.hasRemaining()) {
      tmpBuf = getBuf(f, int32Buf.remaining());
      int32Buf.put(tmpBuf);
      releaseBuffer(f, tmpBuf);
    }
    return int32Buf.getInt();
  }
  
  public static ByteBuffer getBuf(FSDataInputStream f, int maxSize)
      throws IOException {
    ByteBuffer res = null;
    if (useV21) {
      try {
        res = (ByteBuffer) READ_METHOD.invoke(f,
                                              ELASTIC_BYTE_BUFFER_CONSTRUCTOR.newInstance(),
                                              maxSize,
                                              EnumSet.of(Enum.valueOf(ReadOptionCls, "SKIP_CHECKSUMS")));
      } catch (Exception e) {
        byte[] buf = new byte[maxSize];
        f.readFully(buf);
        res = ByteBuffer.wrap(buf);
      } 
    } else {
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
