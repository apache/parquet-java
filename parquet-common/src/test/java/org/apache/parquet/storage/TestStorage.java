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

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import org.junit.Test;

public class TestStorage {

  @Test
  public void testFileSchemeReturnsNioProvider() {
    StorageProvider provider = Storage.select(URI.create("file:///tmp/test.parquet"));
    assertTrue(
        "file:// should return NioStorageProvider",
        provider instanceof org.apache.parquet.storage.impl.NioStorageProvider);
  }

  @Test
  public void testNullUriReturnsNioProvider() {
    StorageProvider provider = Storage.select(null);
    assertTrue(
        "null URI should return NioStorageProvider",
        provider instanceof org.apache.parquet.storage.impl.NioStorageProvider);
  }

  @Test
  public void testRelativePathReturnsNioProvider() {
    StorageProvider provider = Storage.select(URI.create("relative/path"));
    assertTrue(
        "relative path should return NioStorageProvider",
        provider instanceof org.apache.parquet.storage.impl.NioStorageProvider);
  }

  @Test(expected = IllegalStateException.class)
  public void testHdfsSchemeThrowsExceptionWhenHadoopNotAvailable() {
    Storage.select(URI.create("hdfs://namenode:8020/test.parquet"));
  }

  @Test(expected = IllegalStateException.class)
  public void testUnknownSchemeThrowsExceptionWhenHadoopNotAvailable() {
    Storage.select(URI.create("s3://bucket/test.parquet"));
  }

  @Test
  public void testNioProviderBasicOperations() throws IOException {
    StorageProvider provider = Storage.select(URI.create("file:///tmp/test.parquet"));

    assertTrue(provider instanceof org.apache.parquet.storage.impl.NioStorageProvider);
  }

  @Test
  public void testProviderSelectionLogic() {
    StorageProvider fileProvider = Storage.select(URI.create("file:///tmp/test.parquet"));
    assertTrue(
        "file:// should always return NIO provider",
        fileProvider instanceof org.apache.parquet.storage.impl.NioStorageProvider);

    try {
      Storage.select(URI.create("hdfs://namenode:8020/test.parquet"));
      fail("Should throw IllegalStateException for hdfs:// when Hadoop not available");
    } catch (IllegalStateException e) {
    }

    try {
      Storage.select(URI.create("s3://bucket/test.parquet"));
      fail("Should throw IllegalStateException for s3:// when Hadoop not available");
    } catch (IllegalStateException e) {
    }
  }
}
