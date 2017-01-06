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

import org.apache.parquet.SemanticVersion.SemanticVersionParseException;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * There was a bug (PARQUET-251) that caused the statistics metadata
 * for binary columns to be corrupted in the write path.
 *
 * This class is used to detect whether a file was written with this bug,
 * and thus it's statistics should be ignored / not trusted.
 */
public class CorruptStatistics {
  private static final Log LOG = Log.getLog(CorruptStatistics.class);

  private static final AtomicBoolean alreadyLogged = new AtomicBoolean(false);

  // the version in which the bug described by jira: PARQUET-251 was fixed
  // the bug involved writing invalid binary statistics, so stats written prior to this
  // fix must be ignored / assumed invalid
  private static final SemanticVersion PARQUET_251_FIXED_VERSION = new SemanticVersion(1, 8, 0);

  /**
   * Decides if the statistics from a file created by createdBy (the created_by field from parquet format)
   * should be ignored because they are potentially corrupt.
   */
  public static boolean shouldIgnoreStatistics(String createdBy, PrimitiveTypeName columnType) {

    if (columnType != PrimitiveTypeName.BINARY && columnType != PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY) {
      // the bug only applies to binary columns
      return false;
    }

    if (Strings.isNullOrEmpty(createdBy)) {
      // created_by is not populated, which could have been caused by
      // parquet-mr during the same time as PARQUET-251, see PARQUET-297
      warnOnce("Ignoring statistics because created_by is null or empty! See PARQUET-251 and PARQUET-297");
      return true;
    }

    try {
      ParsedVersion version = VersionParser.parse(createdBy);

      if (!"parquet-mr".equals(version.application)) {
        // assume other applications don't have this bug
        return false;
      }

      if (Strings.isNullOrEmpty(version.version)) {
        warnOnce("Ignoring statistics because created_by did not contain a semver (see PARQUET-251): " + createdBy);
        return true;
      }

      SemanticVersion semver = SemanticVersion.parse(version.version);

      if (semver.compareTo(PARQUET_251_FIXED_VERSION) < 0) {
        warnOnce("Ignoring statistics because this file was created prior to "
            + PARQUET_251_FIXED_VERSION
            + ", see PARQUET-251" );
        return true;
      }

      // this file was created after the fix
      return false;
    } catch (RuntimeException e) {
      // couldn't parse the created_by field, log what went wrong, don't trust the stats,
      // but don't make this fatal.
      warnParseErrorOnce(createdBy, e);
      return true;
    } catch (SemanticVersionParseException e) {
      // couldn't parse the created_by field, log what went wrong, don't trust the stats,
      // but don't make this fatal.
      warnParseErrorOnce(createdBy, e);
      return true;
    } catch (VersionParseException e) {
      // couldn't parse the created_by field, log what went wrong, don't trust the stats,
      // but don't make this fatal.
      warnParseErrorOnce(createdBy, e);
      return true;
    }
  }

  private static void warnParseErrorOnce(String createdBy, Throwable e) {
    if(!alreadyLogged.getAndSet(true)) {
      LOG.warn("Ignoring statistics because created_by could not be parsed (see PARQUET-251): " + createdBy, e);
    }
  }

  private static void warnOnce(String message) {
    if(!alreadyLogged.getAndSet(true)) {
      LOG.warn(message);
    }
  }
}
