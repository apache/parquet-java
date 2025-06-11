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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.parquet.SemanticVersion.SemanticVersionParseException;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Not all parquet writers populate the int96 statistics correctly. For example: arrow-rs
 * https://github.com/apache/arrow-rs/blob/3ed9aedabc9e5a90170e43ff818f24a29eafb35b/parquet/src/file/statistics.rs#L212-L215
 * This class is used to detect whether a file was written with a version that has correct int96 statistics.
 */
public class ValidInt96Stats {
  private static final AtomicBoolean alreadyLogged = new AtomicBoolean(false);

  private static final Logger LOG = LoggerFactory.getLogger(ValidInt96Stats.class);


  /**
   * Decides if the statistics from a file created by createdBy (the created_by field from parquet format)
   * should be trusted for INT96 columns.
   *
   * @param createdBy  the created-by string from a file footer
   * @return true if the statistics are valid and can be trusted, false otherwise
   */
  public static boolean hasValidInt96Stats(String createdBy) {
    if (Strings.isNullOrEmpty(createdBy)) {
      warnOnce("Cannot verify INT96 statistics because created_by is null or empty");
      return false;
    }

    try {
      ParsedVersion version = VersionParser.parse(createdBy);
      if ("parquet-mr".equals(version.application)) {
        return true;
      }
      if ("parquet-mr compatible Photon".equals(version.application)) {
        return true;
      }
    } catch (RuntimeException | VersionParseException e) {
      warnParseErrorOnce(createdBy, e);
    }
    return false;
  }

  private static void warnParseErrorOnce(String createdBy, Throwable e) {
    if (!alreadyLogged.getAndSet(true)) {
      LOG.warn("Cannot verify INT96 statistics because created_by could not be parsed: " + createdBy, e);
    }
  }

  private static void warnOnce(String message) {
    if (!alreadyLogged.getAndSet(true)) {
      LOG.warn(message);
    }
  }
}
