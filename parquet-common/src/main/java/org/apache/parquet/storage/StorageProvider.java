/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Simple abstraction for file IO that allows Parquet components to work with
 * different storage back-ends (NIO, Hadoop, cloud SDKs, etc.) without taking
 * an explicit dependency on the back-end libraries.
 *
 * <p>The interface purposefully exposes a very small surface – just the
 * operations currently needed by the Parquet code we intend to decouple from
 * Hadoop.  Additional methods can be added incrementally as other areas are
 * ported.</p>
 *
 * <p>This interface lives in the <code>parquet-common</code> module so that it
 * can be referenced by any other module without creating additional coupling
 * between them.</p>
 */
public interface StorageProvider {

  /**
   * Opens the given path for reading.
   *
   * @param path fully-qualified file path (implementation specific semantics)
   * @return an InputStream that must be closed by the caller
   */
  InputStream openForRead(String path) throws IOException;

  /**
   * Opens the given path for writing. If the file already exists the behaviour
   * depends on the overwrite flag.
   *
   * @param path fully-qualified file path
   * @param overwrite  whether an existing file should be replaced
   * @return an OutputStream that must be closed by the caller
   */
  OutputStream openForWrite(String path, boolean overwrite) throws IOException;

  /**
   * Deletes the file at path if it exists.
   */
  boolean delete(String path) throws IOException;

  /**
   * Renames the file located at source to target.
   */
  boolean rename(String source, String target) throws IOException;
}
