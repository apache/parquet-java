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
package org.apache.parquet.util;

import org.apache.parquet.ParquetRuntimeException;

/**
 * Utility class to handle {@link AutoCloseable} objects.
 */
public final class AutoCloseables {

  public static class ParquetCloseResourceException extends ParquetRuntimeException {

    private ParquetCloseResourceException(Exception e) {
      super("Unable to close resource", e);
    }
  }

  /**
   * Invokes the {@link AutoCloseable#close()} method of each specified objects in a way that guarantees that all the
   * methods will be invoked even if an exception is occurred before.
   *
   * @param autoCloseables the objects to be closed
   * @throws Exception the compound exception built from the exceptions thrown by the close methods
   */
  public static void close(Iterable<AutoCloseable> autoCloseables) throws Exception {
    Exception root = null;
    for (AutoCloseable autoCloseable : autoCloseables) {
      try {
        autoCloseable.close();
      } catch (Exception e) {
        if (root == null) {
          root = e;
        } else {
          root.addSuppressed(e);
        }
      }
    }
    if (root != null) {
      throw root;
    }
  }

  /**
   * Works similarly to {@link #close(Iterable)} but it wraps the thrown exception (if any) into a
   * {@link ParquetCloseResourceException}.
   */
  public static void uncheckedClose(Iterable<AutoCloseable> autoCloseables) throws ParquetCloseResourceException {
    try {
      close(autoCloseables);
    } catch (Exception e) {
      throw new ParquetCloseResourceException(e);
    }
  }

  private AutoCloseables() {}
}
