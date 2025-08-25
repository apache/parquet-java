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

import java.net.URI;

/**
 * Simple helper to select a {@link StorageProvider} implementation based on a
 * Hadoop/NIO constructs. This keeps provider discovery logic outside of call-sites.
 */
public final class Storage {
  private Storage() {}

  /**
   * Return the appropriate provider for the given URI scheme.
   * - file:// or no scheme → NIO provider
   * - hdfs:// → Hadoop provider (if available)
   */
  public static StorageProvider select(URI uri) {
    final String scheme = uri == null ? null : uri.getScheme();

    if (scheme == null || "file".equals(scheme)) {
      return new org.apache.parquet.storage.impl.NioStorageProvider();
    }

    try {
      Class<?> confClass = Class.forName("org.apache.hadoop.conf.Configuration");
      Object conf = confClass.getDeclaredConstructor().newInstance();
      Class<?> providerClass = Class.forName("org.apache.parquet.storage.impl.hadoop.HadoopStorageProvider");
      return (StorageProvider) providerClass.getConstructor(confClass).newInstance(conf);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(
          "Hadoop not on classpath for URI scheme hdfs, please ensure hadoop dependencies are available in classpath",
          e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize HadoopStorageProvider", e);
    }
  }
}
