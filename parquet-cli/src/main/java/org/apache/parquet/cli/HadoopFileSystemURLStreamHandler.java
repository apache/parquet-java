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
package org.apache.parquet.cli;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A {@link URLStreamHandler} for handling Hadoop filesystem URLs,
 * most commonly those with the <i>hdfs</i> scheme.
 */
public class HadoopFileSystemURLStreamHandler extends URLStreamHandler
    implements Configurable {

  private static Configuration defaultConf = new Configuration();

  public static Configuration getDefaultConf() {
    return defaultConf;
  }

  public static void setDefaultConf(Configuration defaultConf) {
    HadoopFileSystemURLStreamHandler.defaultConf = defaultConf;
  }

  private Configuration conf = defaultConf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  protected URLConnection openConnection(URL url) throws IOException {
    return new HadoopFileSystemURLConnection(url);
  }

  class HadoopFileSystemURLConnection extends URLConnection {
    public HadoopFileSystemURLConnection(URL url) {
      super(url);
    }
    @Override
    public void connect() throws IOException {
    }
    @Override
    public InputStream getInputStream() throws IOException {
      Path path = new Path(url.toExternalForm());
      FileSystem fileSystem = path.getFileSystem(conf);
      return fileSystem.open(path);
    }
  }
}
