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
package org.apache.parquet.io.api;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;

final class FileValueValidator {
  private FileValueValidator() {}

  static void validateSchema(GroupType schema) {
    if (!(schema.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.FileLogicalTypeAnnotation)) {
      throw new IllegalArgumentException(
          "Cannot use a FILE value with a group without the FILE logical type: " + schema.getName());
    }
    ((LogicalTypeAnnotation.FileLogicalTypeAnnotation) schema.getLogicalTypeAnnotation()).validate(schema);
  }

  static void validateValue(FileValue value) {
    boolean hasUri = value.getUri() != null && !value.getUri().isEmpty();
    boolean hasInline = value.getInline() != null;

    if (!hasUri && !hasInline) {
      throw new IllegalArgumentException("FILE value must set at least one of 'inline' or non-empty 'uri'");
    }
    if (value.getOffset() != null && !hasUri) {
      throw new IllegalArgumentException("FILE value field 'offset' may only be set together with 'uri'");
    }
    if (value.getSize() != null && !hasUri) {
      throw new IllegalArgumentException("FILE value field 'size' may only be set together with 'uri'");
    }
    if (value.getOffset() != null && value.getSize() == null) {
      throw new IllegalArgumentException("FILE value field 'size' must be set whenever 'offset' is set");
    }
    if (value.getOffset() != null && value.getOffset() < 0) {
      throw new IllegalArgumentException("FILE value field 'offset' must not be negative: " + value.getOffset());
    }
    if (value.getSize() != null && value.getSize() < 0) {
      throw new IllegalArgumentException("FILE value field 'size' must not be negative: " + value.getSize());
    }
  }
}
