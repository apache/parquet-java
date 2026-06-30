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
package org.apache.parquet.hadoop.codec;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class that uses {@code sun.misc.Unsafe#invokeCleaner(ByteBuffer)}
 * reflectively to clean up direct buffers.
 * <p>
 * Strongly inspired by:
 * <a href="https://github.com/apache/tomcat/blob/master/java/org/apache/tomcat/util/buf/ByteBufferUtils.java">Tomcat ByteBufferUtils</a>
 */
public class CleanUtil {
  private static final Logger logger = LoggerFactory.getLogger(CleanUtil.class);

  private static final Object unsafe;
  private static final Method invokeCleanerMethod;

  static {
    final ByteBuffer tempBuffer = ByteBuffer.allocateDirect(0);
    Object unsafeLocal = null;
    Method invokeCleanerMethodLocal = null;
    try {
      final Class<?> clazz = Class.forName("sun.misc.Unsafe");
      final Field theUnsafe = clazz.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      unsafeLocal = theUnsafe.get(null);
      invokeCleanerMethodLocal = clazz.getMethod("invokeCleaner", ByteBuffer.class);
      invokeCleanerMethodLocal.invoke(unsafeLocal, tempBuffer);
    } catch (IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | NoSuchMethodException
        | SecurityException
        | ClassNotFoundException
        | NoSuchFieldException e) {
      logger.warn("Cannot use direct ByteBuffer cleaner, memory leaking may occur", e);
      unsafeLocal = null;
      invokeCleanerMethodLocal = null;
    }
    unsafe = unsafeLocal;
    invokeCleanerMethod = invokeCleanerMethodLocal;
  }

  private CleanUtil() {
    // Hide the default constructor since this is a utility class.
  }

  public static void cleanDirectBuffer(ByteBuffer buf) {
    if (invokeCleanerMethod != null) {
      try {
        invokeCleanerMethod.invoke(unsafe, buf);
      } catch (IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | SecurityException e) {
        logger.warn("Error while cleaning up the DirectBuffer", e);
      }
    }
  }
}
