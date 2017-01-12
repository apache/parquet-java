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
package org.apache.parquet.hadoop;

import java.util.Objects;

import org.apache.parquet.format.PageHeader;

/**
 * PageHeader information along with offset into file.
 */
public class PageHeaderWithOffset {
  final PageHeader pageHeader;
  final long offset;

  public PageHeaderWithOffset(PageHeader pageHeader, long offset) {
    this.pageHeader = pageHeader;
    this.offset = offset;
  }

  public PageHeaderWithOffset(PageHeader pageHeader, long offset, long headerByteSize) {
    this.pageHeader = pageHeader;
    this.offset = offset;
  }

  public PageHeader getPageHeader() {
    return pageHeader;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pageHeader, offset);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      if (obj instanceof PageHeaderWithOffset) {
        PageHeaderWithOffset that = (PageHeaderWithOffset)obj;
        return offset == that.offset &&
          pageHeader.equals(that.pageHeader);
      }
    }
    return false;
  }
}
