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

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Utility class to validate different types of Parquet metadata (e.g. footer, page headers etc.).
 */
public class MetadataValidator {

  /**
   * A specific IOException thrown when invalid values are found in the Parquet file metadata (including the footer,
   * page header etc.).
   */
  public static class InvalidParquetMetadataException extends IOException {
    private <T> InvalidParquetMetadataException(String metaName, T value) {
      super("Metadata " + metaName + " is invalid: " + value);
    }
  }

  static PageHeader validate(PageHeader pageHeader) throws InvalidParquetMetadataException {
    validateValue(size -> size >= 0, pageHeader.getCompressed_page_size(), "pageHeader.compressed_page_size");
    return pageHeader;
  }

  private static <T> void validateValue(Predicate<? super T> validator, T value, String metaName)
      throws InvalidParquetMetadataException {
    if (!validator.test(value)) {
      throw new InvalidParquetMetadataException(metaName, value);
    }
  }

  private MetadataValidator() {
    // Private constructor to prevent instantiation
  }

}
