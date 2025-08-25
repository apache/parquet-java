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

package org.apache.parquet.storage.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import org.apache.parquet.storage.StorageProvider;

/**
 * A StorageProvider that relies solely on the Java NIO standard library. It
 * can work with any FileSystem implementation available
 * on the class-path.
 */
public class NioStorageProvider implements StorageProvider {

  public NioStorageProvider() {}

  private static Path toPath(String s) {
    try {
      java.net.URI uri = java.net.URI.create(s);
      if (uri.getScheme() != null) {
        return Paths.get(uri);
      }
    } catch (IllegalArgumentException ignore) {
    }
    return Paths.get(s);
  }

  @Override
  public InputStream openForRead(String path) throws IOException {
    return Files.newInputStream(toPath(path));
  }

  @Override
  public OutputStream openForWrite(String path, boolean overwrite) throws IOException {
    Path p = toPath(path);
    if (overwrite) {
      return Files.newOutputStream(
          p, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    } else {
      return Files.newOutputStream(p, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }
  }

  @Override
  public boolean delete(String path) throws IOException {
    return Files.deleteIfExists(toPath(path));
  }

  @Override
  public boolean rename(String source, String target) throws IOException {
    Path src = toPath(source);
    Path tgt = toPath(target);
    if (Files.exists(tgt)) return false;
    try {
      Files.move(src, tgt, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(src, tgt);
    }
    return true;
  }
}
