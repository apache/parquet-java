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
package org.apache.parquet.hadoop.util;

import org.apache.hadoop.fs.FSDataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;

import org.apache.parquet.Log;
import parquet.org.apache.thrift.TBase;
import parquet.org.apache.thrift.TException;
import parquet.org.apache.thrift.protocol.*;
import parquet.org.apache.thrift.transport.TIOStreamTransport;
import parquet.org.apache.thrift.transport.TTransport;
import parquet.org.apache.thrift.transport.TTransportException;

public class CompatibilityUtil {
  private static final boolean useV21;
  private static final Log LOG = Log.getLog(CompatibilityUtil.class);
  public static final V21FileAPI fileAPI;
  private static final int MAX_SIZE = 1 << 20;
  private static final Object bufferPool;
  
  private static class V21FileAPI {
    private final Constructor<?> ELASTIC_BYTE_BUFFER_CONSTRUCTOR;
    private final Class<?> ElasticByteBufferCls;
    private final Class<?> ByteBufferCls;
    private final Class<? extends Enum> ReadOptionCls;
    private final Method READ_METHOD;
    private final Method RELEASE_BUFFER_METHOD;
    private final Method GET_BUFFER_METHOD;
    private final Method PUT_BUFFER_METHOD;
    private final Class<?> FSDataInputStreamCls;
    
    private V21FileAPI() throws ClassNotFoundException, NoSuchMethodException, SecurityException {
      final String PACKAGE = "org.apache.hadoop";
      ElasticByteBufferCls = Class.forName(PACKAGE + ".io.ElasticByteBufferPool");
      ELASTIC_BYTE_BUFFER_CONSTRUCTOR = ElasticByteBufferCls.getConstructor();
      ByteBufferCls = Class.forName(PACKAGE + ".io.ByteBufferPool");
      FSDataInputStreamCls = Class.forName(PACKAGE + ".fs.FSDataInputStream");
      ReadOptionCls = (Class<Enum>)Class.forName(PACKAGE + ".fs.ReadOption");
      READ_METHOD = FSDataInputStreamCls.getMethod("read", ByteBufferCls, int.class, EnumSet.class);
      RELEASE_BUFFER_METHOD = FSDataInputStreamCls.getMethod("releaseBuffer", ByteBuffer.class);
      GET_BUFFER_METHOD = ElasticByteBufferCls.getMethod("getBuffer", boolean.class, int.class);
      PUT_BUFFER_METHOD = ElasticByteBufferCls.getMethod("putBuffer", ByteBuffer.class);
    }
  }
  
  static {
    boolean v21 = true;
    try {
      Class.forName("org.apache.hadoop.io.ElasticByteBufferPool");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }
    
    useV21 = v21;
    try {
      if (v21) {
        fileAPI = new V21FileAPI();
        bufferPool = fileAPI.ELASTIC_BYTE_BUFFER_CONSTRUCTOR.newInstance();
      } else {
        fileAPI = null;
        bufferPool = null;
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class", e);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("Can't find constructor ", e);
    } catch (InstantiationException e) {
      throw new IllegalArgumentException("Can't create instance ", e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Can't create instance ", e);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Can't create instance ", e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException("Can't create instance ", e);
    }
  }
  
  public static void releaseBuffer(FSDataInputStream f, ByteBuffer buf) {
    if (useV21) {
      try {
        fileAPI.RELEASE_BUFFER_METHOD.invoke(f, buf);
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
        res = (ByteBuffer) fileAPI.READ_METHOD.invoke(f,
                                              fileAPI.ELASTIC_BYTE_BUFFER_CONSTRUCTOR.newInstance(),
                                              maxSize,
                                              EnumSet.of(Enum.valueOf(fileAPI.ReadOptionCls, "SKIP_CHECKSUMS")));
      } catch (Exception e) {
        byte[] buf = new byte[maxSize];
        f.read(buf,0,  maxSize);
        res = ByteBuffer.wrap(buf);
      } 
    } else {
      byte[] buf = new byte[maxSize];
      int size = f.read(buf,0,  maxSize);
      res = ByteBuffer.wrap(buf, 0, size);
    }
    
    if (res == null) {
      throw new EOFException("Null ByteBuffer returned");
    }
    return res;
  }

  // Caller must allocate the buffer
  public static ByteBuffer getBuf(FSDataInputStream f, ByteBuffer readBuf, int maxSize) throws IOException {
    Class<?>[] ZCopyArgs = {ByteBuffer.class};
    int res=0;
    int l=readBuf.remaining();
    if (useV21) {
      try {
        if (readBuf.hasArray()) {
          res = f.read(readBuf.array());
        } else {
          // TODO = figure out how to avoid this copy using the new Hadoop 2.0 API if possible
          // this is in the compatibility section, so it might deliberately only have
          // access to the old API
          byte[] temp = new byte[readBuf.remaining()];
          res = f.read(temp);
          readBuf.put(temp);
        }
      }catch (UnsupportedOperationException e) {
        byte[] buf = new byte[maxSize];
        res=f.read(buf);
        readBuf.put(buf, 0, maxSize);
      }
    } else {
      byte[] buf = new byte[maxSize];
      res=f.read(buf);
      readBuf.put(buf, 0, maxSize);
    }

    if (res == 0) {
      throw new EOFException("Null ByteBuffer returned");
    }
    return readBuf;
  }
  
  public static void bbCopy(ByteBuffer dst, ByteBuffer src) {
      final int n = Math.min(dst.remaining(), src.remaining());
    for (int i = 0; i < n; i++) {
      dst.put(src.get());
    }
  }
  
  public static <T extends TBase<?,?>> T read(FSDataInputStream f, T tbase)
      throws IOException {
    try {
      // Reverting to using TIOStreamTransport instead of the FSDistTransport
      // implementation below. FSDistTransport is 4x slower when reading footers.
      tbase.read(new TCompactProtocol(new TIOStreamTransport(f)));
      return tbase;
    } catch (TException e) {
      throw new IOException("can not read " + tbase.getClass() + ": "
          + e.getMessage(), e);
    }
  }
  
  private static final class FSDISTransport extends TTransport {
    private final FSDataInputStream fsdis;
    // ByteBuffer-based API
    private ByteBuffer tbuf;
    private ByteBuffer slice;

    private FSDISTransport(FSDataInputStream f) {
      super();
      fsdis = f;
    }

    @Override
    public boolean isOpen() {
      return true; // TODO
    }

    @Override
    public boolean peek() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void open() throws TTransportException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] bytes, int i, int i2) throws TTransportException {
      throw new UnsupportedOperationException("ByteBuffer API to be used");
    }

    @Override
    public int readAll(byte[] buf, int off, int len) throws TTransportException {
      ByteBuffer tmpBuf = readFully(len);
      tmpBuf.get(buf, off, len);
      return len;
    }

    @Override
    public void write(byte[] buf) throws TTransportException {
      throw new UnsupportedOperationException("Read-Only implementation");
    }

    @Override
    public void write(byte[] bytes, int i, int i2) throws TTransportException {
      throw new UnsupportedOperationException("Read-Only implementation");
    }

    @Override
    public void flush() throws TTransportException {
      throw new UnsupportedOperationException("Read-Only implementation");
    }

    @Override
    public byte[] getBuffer() {
      if (tbuf == null) {
        return null;
      }
      int pos = tbuf.position();
      tbuf.rewind();
      byte[] buf = new byte[tbuf.remaining()];
      tbuf.get(buf);
      tbuf.position(pos);
      return buf;
    }

    @Override
    public int getBufferPosition() {
      if (tbuf == null) {
        return 0;
      }
      return tbuf.position();
    }

    @Override
    public int getBytesRemainingInBuffer() {
      if (tbuf == null) {
        return 0;
      }
      return tbuf.remaining();
    }

    @Override
    public void consumeBuffer(int len) {
      if (tbuf == null) {
        return;
      }
      int pos = tbuf.position();
      tbuf.position(pos + len);
      return;
    }

    public byte readByte() throws TTransportException {
      try {
        for (;;) {
          if (tbuf == null) {
            tbuf = getBuf(fsdis, MAX_SIZE);
          }
          if (tbuf.hasRemaining()) {
            return tbuf.get();
          } else {
            release(tbuf);
          }
        }
      } catch (IOException ioe) {
        throw new TTransportException("Hadoop FS", ioe);
      } finally {
        release(tbuf);
      }
    }

    public ByteBuffer readFully(int size) throws TTransportException {
      try {
        ByteBuffer newBuf = null; // crossing boundaries
        for (;;) {
          if (tbuf == null) {
            tbuf = getBuf(fsdis, MAX_SIZE);
          }
          if (newBuf == null) {
            // serve slice from I/O buffer?
            if (tbuf.remaining() >= size) {
              final int lim = tbuf.limit();
              tbuf.limit(tbuf.position() + size);
              slice = tbuf.slice();
              tbuf.position(tbuf.limit());
              tbuf.limit(lim);
              return slice;
            } else {
              try {
                newBuf = (ByteBuffer)fileAPI.GET_BUFFER_METHOD.invoke(bufferPool, false, size);
              } catch (IllegalAccessException e) {
                throw new TTransportException("Hadoop FS", e);
              } catch (IllegalArgumentException e) {
                throw new TTransportException("Hadoop FS", e);
              } catch (InvocationTargetException e) {
                throw new TTransportException("Hadoop FS", e);
              }
              newBuf.limit(size).position(0);
            }
          }
          // no zero copy
          bbCopy(newBuf, tbuf);
          release(tbuf);
          if (!newBuf.hasRemaining()) {
            newBuf.flip();
            if (newBuf.remaining() != size) {
              throw new TTransportException("boom");
            }
            return newBuf;
          }
        }
      } catch (IOException ioe) {
        throw new TTransportException("Hadoop FS", ioe);
      }
    }

    public void release(ByteBuffer b) {
      if (b == null) {
        return;
      } else if (b == slice) {
        slice = null;
      } else if (b == tbuf) {
        if (!tbuf.hasRemaining()) {
          releaseBuffer(fsdis, tbuf);
          tbuf = null;
        }
      } else {
        try {
          fileAPI.PUT_BUFFER_METHOD.invoke(bufferPool, b);
        } catch (IllegalAccessException e) {
          throw new IllegalArgumentException("Can't call method", e);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Can't call method", e);
        } catch (InvocationTargetException e) {
          throw new IllegalArgumentException("Can't call method", e);
        }
      }
    }
  }
}
