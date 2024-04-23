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
package org.apache.parquet.column.impl;

/**
 * Interface to manage the current error status. It is used to share the status of all the different (column, page,
 * etc.) writer/reader instances.
 */
interface StatusManager {

  /**
   * Creates an instance of the default {@link StatusManager} implementation.
   *
   * @return the newly created {@link StatusManager} instance
   */
  static StatusManager create() {
    return new StatusManager() {
      private boolean aborted;

      @Override
      public void abort() {
        aborted = true;
      }

      @Override
      public boolean isAborted() {
        return aborted;
      }
    };
  }

  /**
   * To be invoked if the current process is to be aborted. For example in case of an exception is occurred during
   * writing a page.
   */
  void abort();

  /**
   * Returns whether the current process is aborted.
   *
   * @return {@code true} if the current process is aborted, {@code false} otherwise
   */
  boolean isAborted();
}
