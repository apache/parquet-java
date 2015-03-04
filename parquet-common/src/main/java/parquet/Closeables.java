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
package parquet;

import java.io.Closeable;
import java.io.IOException;

/**
 * Utility for working with {@link java.io.Closeable}ss
 */
public final class Closeables {
  private Closeables() { }

  private static final Log LOG = Log.getLog(Closeables.class);

  /**
   * Closes a (potentially null) closeable.
   * @param c can be null
   * @throws IOException if c.close() throws an IOException.
   */
  public static void close(Closeable c) throws IOException {
    if (c == null) { return; }
    c.close();
  }

  /**
   * Closes a (potentially null) closeable, swallowing any IOExceptions thrown by
   * c.close(). The exception will be logged.
   * @param c can be null
   */
  public static void closeAndSwallowIOExceptions(Closeable c) {
    if (c == null) { return; }
    try {
      c.close();
    } catch (IOException e) {
      LOG.warn("Encountered exception closing closeable", e);
    }
  }
}
