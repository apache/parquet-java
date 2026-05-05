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

package org.apache.parquet.format;

/**
 * Utility class to validate different types of Parquet metadata (e.g. footer, page headers etc.).
 */
public class MetadataValidator {

  static PageHeader validate(PageHeader pageHeader) {
    int compressed_page_size = pageHeader.getCompressed_page_size();
    validateValue(
        compressed_page_size >= 0,
        String.format("Compressed page size must not be negative but was: %s", compressed_page_size));
    return pageHeader;
  }

  private static <T> void validateValue(boolean valid, String message) {
    if (!valid) {
      throw new InvalidParquetMetadataException(message);
    }
  }

  private MetadataValidator() {
    // Private constructor to prevent instantiation
  }
}
