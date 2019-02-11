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
 * A Helper class which use reflections to clean up DirectBuffer. It's implemented for
 * better compatibility with both java8 and java9+, because the Cleaner class is moved to
 * another place since java9+.
 */
public class CleanUtil {
  private static final Logger logger = LoggerFactory.getLogger(CleanUtil.class);
  private static final Field CLEANER_FIELD;
  private static final Method CLEAN_METHOD;

  static {
    ByteBuffer buf = null;
    Field cleanerField = null;
    Method cleanMethod = null;
    try {
      buf = ByteBuffer.allocateDirect(1);
      cleanerField = buf.getClass().getDeclaredField("cleaner");
      cleanerField.setAccessible(true);
      Object cleaner = cleanerField.get(buf);
      cleanMethod = cleaner.getClass().getDeclaredMethod("clean");
    } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException e) {
      logger.warn("Initialization failed for cleanerField or cleanMethod", e);
    } finally {
      clean(buf);
    }
    CLEANER_FIELD = cleanerField;
    CLEAN_METHOD = cleanMethod;
  }

  public static void clean(ByteBuffer buffer) {
    if (CLEANER_FIELD == null || CLEAN_METHOD == null) {
      return;
    }
    try {
      Object cleaner = CLEANER_FIELD.get(buffer);
      CLEAN_METHOD.invoke(cleaner);
    } catch (IllegalAccessException | InvocationTargetException | NullPointerException e) {
      // Ignore clean failure
      logger.warn("Clean failed for buffer " + buffer.getClass().getSimpleName(), e);
    }
  }
}
