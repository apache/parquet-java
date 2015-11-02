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
import org.apache.parquet.ShouldNeverHappenException;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CompatibilityUtil {

  // Will be set to true if the implementation of FSDataInputSteam supports
  // the 2.x APIs, in particular reading using a provided ByteBuffer
  private static final boolean useV21;
  public static final V21FileAPI fileAPI;

  private static class V21FileAPI {
    private final Method PROVIDE_BUF_READ_METHOD;
    private final Class<?> FSDataInputStreamCls;

    private V21FileAPI() throws ReflectiveOperationException {
      final String PACKAGE = "org.apache.hadoop";
      FSDataInputStreamCls = Class.forName(PACKAGE + ".fs.FSDataInputStream");
      PROVIDE_BUF_READ_METHOD = FSDataInputStreamCls.getMethod("read", ByteBuffer.class);
    }
  }
  
  static {
    // Test to see if a class from the Hadoop 2.x API is available
    boolean v21 = true;
    try {
      Class.forName("org.apache.hadoop.io.DirectDecompressor");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }

    boolean interfaceNotActuallyImplemented;
    try {
      if (v21) {
        fileAPI = new V21FileAPI();
        ByteBuffer readBuf = ByteBuffer.allocateDirect(100);
        // See if the implementation of the filesystem can actually provides functionality in the
        // new interface or just throws UnsupportedOperationException
        try {
          invoke(fileAPI.PROVIDE_BUF_READ_METHOD, "Unexpected error reading into a ByteBuffer", readBuf);
          interfaceNotActuallyImplemented = false;
        } catch (UnsupportedOperationException e) {
          interfaceNotActuallyImplemented = true;
        }
        if (interfaceNotActuallyImplemented) {
          useV21 = false;
        } else {
          useV21 = true;
        }
      } else {
        fileAPI = null;
        useV21 = false;
      }

    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Error finding appropriate interfaces using reflection.", e);
    }
  }

  private static Object invoke(Method method, String errorMsg, Object instance, Object... args) {
    try {
      return method.invoke(instance, args);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(errorMsg, e);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException(errorMsg, e);
    }
  }

  public static int getBuf(FSDataInputStream f, ByteBuffer readBuf, int maxSize) throws IOException {
    int res = 0;
    if (useV21) {
      try {
        res = (Integer) invoke(fileAPI.PROVIDE_BUF_READ_METHOD, "Unexpected error reading into a ByteBuffer", readBuf);
      } catch (UnsupportedOperationException e) {
        // checked for this earlier and set useV21 to false if this exception was thrown
        // by the current implementation, this should never be thrown
        throw new ShouldNeverHappenException(e);
      }
    } else {
      byte[] buf = new byte[maxSize];
      res = f.read(buf);
      readBuf.put(buf, 0, maxSize);
    }

    if (res == 0) {
      throw new EOFException("Null ByteBuffer returned");
    }
    return res;
  }
}
