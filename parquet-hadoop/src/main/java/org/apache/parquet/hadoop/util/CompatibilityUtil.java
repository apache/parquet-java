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
package org.apache.parquet.hadoop.util;

import org.apache.parquet.Log;

public class CompatibilityUtil {

  private static final String READER_V2_CLASS = "org.apache.parquet.hadoop.util.CompatibilityReaderV2";

  private static final Log LOG = Log.getLog(CompatibilityUtil.class);

  public static CompatibilityReader getHadoopReader(boolean useV2) {
    if (!useV2) {
      return new CompatibilityReaderV1();
    }

    if (!isHadoop2x()) {
      LOG.info("Can't read Hadoop 2x classes, will be using 1x read APIs");
      return new CompatibilityReaderV1();
    }

    return newV2Reader();
  }

  // Test to see if a class from the Hadoop 2.x API is available
  // If it is, we try to instantiate the V2 CompatibilityReader.
  private static boolean isHadoop2x() {
    boolean v2 = true;
    try {
      Class.forName("org.apache.hadoop.io.compress.DirectDecompressor");
    } catch (ClassNotFoundException cnfe) {
      v2 = false;
    }
    return v2;
  }

  private static CompatibilityReader newV2Reader() {
    try {
      Class<?> reader = Class.forName(READER_V2_CLASS);
      return (CompatibilityReader)reader.newInstance();
    } catch (ReflectiveOperationException e) {
      LOG.warn("Unable to instantiate Hadoop V2 compatibility reader class: " + READER_V2_CLASS + " , will be using 1x read APIs", e);
      return new CompatibilityReaderV1();
    }
  }
}
