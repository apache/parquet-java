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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.Log;
import org.apache.parquet.ShouldNeverHappenException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CompatibilityUtil {

  private static class V21FileAPI {
    private final Method PROVIDE_BUF_READ_METHOD;
    private final Class<?> FSDataInputStreamCls;

    private V21FileAPI() throws ReflectiveOperationException {
      final String PACKAGE = "org.apache.hadoop";
      FSDataInputStreamCls = Class.forName(PACKAGE + ".fs.FSDataInputStream");
      PROVIDE_BUF_READ_METHOD = FSDataInputStreamCls.getMethod("read", ByteBuffer.class);
    }
  }

  // Will be set to true if the implementation of FSDataInputSteam supports
  // the 2.x APIs, in particular reading using a provided ByteBuffer
  private boolean useV2;
  private V21FileAPI fileAPI;

  private static final Log LOG = Log.getLog(CompatibilityUtil.class);

  public CompatibilityUtil(boolean useV2) {
    if ( useV2 && !isHadoop2x() ) {
      LOG.info("Can't read Hadoop 2x classes, will be using 1x read APIs");
      this.useV2 = false;
    } else {
      this.useV2 = useV2;
    }

    initializeFileAPI(this.useV2);
  }

  private boolean isHadoop2x() {
    // Test to see if a class from the Hadoop 2.x API is available
    boolean v2 = true;
    try {
      Class.forName("org.apache.hadoop.io.compress.DirectDecompressor");
    } catch (ClassNotFoundException cnfe) {
      v2 = false;
    }
    return v2;
  }

  private void initializeFileAPI(boolean useV21) {
    try {
      if (useV21) {
        fileAPI = new V21FileAPI();
      } else {
        fileAPI = null;
      }

    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Error finding appropriate interfaces using reflection.", e);
    }
  }

  /**
   * This method attempts to read into the provided readBuffer, readBuffer.remaining() bytes.
   * If the underlying InputStream supports read directly into ByteBuffer we go ahead and invoke that.
   * Else we fall back to directly calling readFully() on the underlying stream.
   * @return Number of bytes read - should be readBuf.remaining()
   */
  public int getBuf(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int res;
    if (useV2) {
      res = readWithByteBuffer(f, readBuf);
    } else {
      if (readBuf.hasArray()) {
        res = readWithExistingArray(f, readBuf);
      } else {
        res = readWithNewArray(f, readBuf);
      }
    }
    return res;
  }

  private int readWithByteBuffer(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int remaining = readBuf.remaining();
    try {
      while (readBuf.hasRemaining()) {
        fileAPI.PROVIDE_BUF_READ_METHOD.invoke(f, readBuf);
      }
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof UnsupportedOperationException) {
        // the FSDataInputStream docs say specifically that implementations
        // can choose to throw UnsupportedOperationException, so this should
        // be a reasonable check to make to see if the interface is
        // present but not implemented and we should be falling back
        useV2 = false;
        return getBuf(f, readBuf);
      } else if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        // To handle any cases where a Runtime exception occurs and provide
        // some additional context information. A stacktrace would just give
        // a line number, this at least tells them we were using the version
        // of the read method designed for using a ByteBuffer.
        throw new IOException("Error reading out of an FSDataInputStream " +
          "using the Hadoop 2 ByteBuffer based read method.", e.getCause());
      }
    } catch (IllegalAccessException e) {
      // This method is public because it is defined in an interface,
      // there should be no problems accessing it
      throw new ShouldNeverHappenException(e);
    }

    return remaining;
  }

  private int readWithExistingArray(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int initPos = readBuf.position();
    int remaining = readBuf.remaining();
    f.readFully(readBuf.array(), readBuf.arrayOffset(), readBuf.remaining());
    readBuf.position(initPos + remaining);
    return remaining;
  }

  private int readWithNewArray(FSDataInputStream f, ByteBuffer readBuf) throws IOException {
    int remaining = readBuf.remaining();
    byte[] buf = new byte[readBuf.remaining()];
    f.readFully(buf);
    readBuf.put(buf, 0, remaining);
    return remaining;
  }

}
