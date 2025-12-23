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

package org.apache.parquet.storage.impl.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.storage.StorageProvider;

/**
 * Hadoop-backed implementation of {@link StorageProvider}. Lives in the
 * parquet-hadoop module, under the same {@code org.apache.parquet.storage.impl}
 * hierarchy as other providers.
 */
public class HadoopStorageProvider implements StorageProvider {

  private final FileSystem fs;

  /**
   * Uses default Hadoop Configuration.
   */
  public HadoopStorageProvider() throws IOException {
    this(new Configuration());
  }

  public HadoopStorageProvider(Configuration conf) throws IOException {
    this.fs = FileSystem.get(conf);
  }

  @Override
  public InputStream openForRead(String path) throws IOException {
    Path p = new Path(path);
    FSDataInputStream in = fs.open(p);
    // Deliberately return the Hadoop stream as InputStream; callers should
    // not rely on Hadoop-specific methods.
    return in;
  }

  @Override
  public OutputStream openForWrite(String path, boolean overwrite) throws IOException {
    Path p = new Path(path);
    FSDataOutputStream out = fs.create(p, overwrite);
    return out;
  }

  @Override
  public boolean delete(String path) throws IOException {
    return fs.delete(new Path(path), false);
  }

  @Override
  public boolean rename(String source, String target) throws IOException {
    return fs.rename(new Path(source), new Path(target));
  }
}
