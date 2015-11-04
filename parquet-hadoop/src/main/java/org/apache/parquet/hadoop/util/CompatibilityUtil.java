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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class CompatibilityUtil {

  // Will be set to true if the implementation of FSDataInputSteam supports
  // the 2.x APIs, in particular reading using a provided ByteBuffer
  private static boolean useV21;
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
      Class.forName("org.apache.hadoop.io.compress.DirectDecompressor");
    } catch (ClassNotFoundException cnfe) {
      v21 = false;
    }

    useV21 = v21;
    try {
      if (v21) {
        fileAPI = new V21FileAPI();
      } else {
        fileAPI = null;
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
    int res;
    if (useV21) {
      try {
        res = (Integer) fileAPI.PROVIDE_BUF_READ_METHOD.invoke(f, readBuf);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof UnsupportedOperationException) {
          // the FSDataInputStream docs say specifically that implementations
          // can choose to throw UnsupportedOperationException, so this should
          // be a reasonable check to make to see if the interface is
          // present but not implemented and we should be falling back
          useV21 = false;
          return getBuf(f, readBuf, maxSize);
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
    } else {
      byte[] buf = new byte[maxSize];
      res = f.read(buf);
      readBuf.put(buf, 0, res);
    }
    return res;
  }
}
