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
package org.apache.parquet.internal.hadoop.metadata;

/**
 * Reference to an index (OffsetIndex and ColumnIndex) for a row-group containing the offset and length values so the
 * reader can read the referenced data.
 */
public class IndexReference {
  private final long offset;
  private final int length;

  public IndexReference(long offset, int length) {
    this.offset = offset;
    this.length = length;
  }

  public long getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }
}
