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
package org.apache.parquet.column.page;

import java.util.OptionalInt;

/**
 * one page in a chunk
 */
abstract public class Page {

  private final int compressedSize;
  private final int uncompressedSize;

  Page(int compressedSize, int uncompressedSize) {
    super();
    this.compressedSize = compressedSize;
    this.uncompressedSize = uncompressedSize;
  }

  public int getCompressedSize() {
    return compressedSize;
  }

 /**
  * @return the uncompressed size of the page when the bytes are compressed
  */
  public int getUncompressedSize() {
    return uncompressedSize;
  }

  // Note: the following field is only used for testing purposes and are NOT used in checksum
  // verification. There crc value here will merely be a copy of the actual crc field read in
  // ParquetFileReader.Chunk.readAllPages()
  private OptionalInt crc = OptionalInt.empty();

  // Visible for testing
  public void setCrc(int crc) {
    this.crc = OptionalInt.of(crc);
  }

  // Visible for testing
  public OptionalInt getCrc() {
    return crc;
  }
}
