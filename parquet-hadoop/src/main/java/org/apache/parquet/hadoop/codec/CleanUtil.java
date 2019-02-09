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

public class CleanUtil {
  private static final Logger logger = LoggerFactory.getLogger(CleanUtil.class);
  private static final Field CLEANER_FIELD;
  private static final Method CLEAN_METHOD;

  static {
    ByteBuffer buf = null;
    try {
      buf = ByteBuffer.allocateDirect(1);
      CLEANER_FIELD = buf.getClass().getDeclaredField("cleaner");
      CLEANER_FIELD.setAccessible(true);
      Object cleaner = CLEANER_FIELD.get(buf);
      CLEAN_METHOD = cleaner.getClass().getDeclaredMethod("clean");
    } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException e) {
      clean(buf);
      throw new IllegalStateException("No available cleaner found.", e);
    }
  }

  public static void clean(ByteBuffer buffer) {
    try {
      Object cleaner = CLEANER_FIELD.get(buffer);
      CLEAN_METHOD.invoke(cleaner);
    } catch (IllegalAccessException | InvocationTargetException e) {
      // Ignore clean failure
    }
  }
}
