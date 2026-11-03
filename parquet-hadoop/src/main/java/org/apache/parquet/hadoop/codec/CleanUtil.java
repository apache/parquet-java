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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class which uses {@code sun.misc.Unsafe.invokeCleaner} to explicitly free
 * direct ByteBuffers.
 * <p>
 * {@code sun.misc.Unsafe} is looked up reflectively to avoid a compile-time dependency on the
 * {@code jdk.unsupported} module. If it is not available (for example, on JVMs that block access
 * to it), cleaning is a no-op and direct buffers are freed by the garbage collector instead.
 */
public class CleanUtil {
  private static final Logger logger = LoggerFactory.getLogger(CleanUtil.class);

  // Unsafe.invokeCleaner(ByteBuffer) bound to the Unsafe instance, or null if unavailable
  private static final MethodHandle INVOKE_CLEANER;

  static {
    MethodHandle invokeCleaner = null;
    try {
      final Class<?> clazz = Class.forName("sun.misc.Unsafe");
      final Field theUnsafe = clazz.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      invokeCleaner = MethodHandles.lookup()
          .findVirtual(clazz, "invokeCleaner", MethodType.methodType(void.class, ByteBuffer.class))
          .bindTo(theUnsafe.get(null));
    } catch (ReflectiveOperationException | RuntimeException e) {
      logger.warn("Cannot use direct ByteBuffer cleaner, memory leaking may occur", e);
    }
    INVOKE_CLEANER = invokeCleaner;
  }

  private CleanUtil() {
    // Hide the default constructor since this is a utility class.
  }

  public static void cleanDirectBuffer(ByteBuffer buf) {
    if (INVOKE_CLEANER != null) {
      try {
        INVOKE_CLEANER.invokeExact(buf);
      } catch (Error e) {
        throw e;
      } catch (Throwable t) {
        // e.g. IllegalArgumentException if buf is not direct, or is a slice or duplicate
        logger.warn("Error while cleaning up the DirectBuffer", t);
      }
    }
  }
}
