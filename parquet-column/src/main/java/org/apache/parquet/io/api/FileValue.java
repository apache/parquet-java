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

/** A value represented by a Parquet {@code FILE}-annotated group. */
public final class FileValue {
  private final String uri;
  private final Long offset;
  private final Long size;
  private final String contentType;
  private final String checksum;
  private final Binary inline;

  private FileValue(Builder builder) {
    this.uri = builder.uri;
    this.offset = builder.offset;
    this.size = builder.size;
    this.contentType = builder.contentType;
    this.checksum = builder.checksum;
    this.inline = builder.inline;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getUri() {
    return uri;
  }

  public Long getOffset() {
    return offset;
  }

  public Long getSize() {
    return size;
  }

  public String getContentType() {
    return contentType;
  }

  public String getChecksum() {
    return checksum;
  }

  public Binary getInline() {
    return inline;
  }

  /** Builder for {@link FileValue}. */
  public static final class Builder {
    private String uri;
    private Long offset;
    private Long size;
    private String contentType;
    private String checksum;
    private Binary inline;

    private Builder() {}

    public Builder withUri(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder withOffset(long offset) {
      this.offset = offset;
      return this;
    }

    public Builder withSize(long size) {
      this.size = size;
      return this;
    }

    public Builder withContentType(String contentType) {
      this.contentType = contentType;
      return this;
    }

    public Builder withChecksum(String checksum) {
      this.checksum = checksum;
      return this;
    }

    public Builder withInline(Binary inline) {
      this.inline = inline;
      return this;
    }

    public FileValue build() {
      return new FileValue(this);
    }
  }
}
