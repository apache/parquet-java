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
package org.apache.parquet;

import java.io.InputStream;
import java.util.Properties;

/**
 * The version of the library
 *
 * parquet-mr version 1.0.0-SNAPSHOT (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)
 *
 * @author Julien Le Dem
 */
public class Version {
  private static final Log LOG = Log.getLog(Version.class);

  public static final String VERSION_NUMBER;
  public static final String FULL_VERSION;

  static {
    String versionNumber = null;
    String fullVersion = null;

    InputStream in = Version.class.getResourceAsStream("/META-INF/maven/org.apache.parquet/parquet-common/version.properties");
    if (in != null) {
      try {
        Properties props = new Properties();
        props.load(in);
        versionNumber = props.getProperty("versionNumber");
        fullVersion = props.getProperty("fullVersion");
      } catch (Exception e) {
        LOG.warn("can't read version information", e);
      }
    } else {
      LOG.warn("can't read version information");
    }

    VERSION_NUMBER = versionNumber;
    FULL_VERSION = fullVersion;
  }

  public static void main(String[] args) {
    System.out.println(FULL_VERSION);
  }
}
