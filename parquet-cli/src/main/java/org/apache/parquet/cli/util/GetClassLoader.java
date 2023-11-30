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

package org.apache.parquet.cli.util;

import java.net.URL;
import java.net.URLClassLoader;
import java.security.PrivilegedAction;
import java.util.List;

public class GetClassLoader implements PrivilegedAction<ClassLoader> {
  private final URL[] urls;

  public GetClassLoader(List<URL> urls) {
    this.urls = urls.toArray(new URL[0]);
  }

  @Override
  public ClassLoader run() {
    return new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
  }
}
