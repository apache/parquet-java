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
package org.apache.parquet.column.values.symboltable;

/**
 * Compresses values into a stream of codes over a symbol table.
 *
 * <p>Kept separate from the table itself because a table can be paired with more than one code
 * stream framing, and because the encode side needs lookup structures that never reach a file.
 */
public interface CodeStreamEncoder {

  /**
   * Upper bound on the compressed size of a value, so a caller can size a destination buffer.
   *
   * <p>Must account for escapes, which make the worst case larger than the input.
   */
  int maxCompressedLength(int rawLength);

  /**
   * Compresses one value.
   *
   * @param source          the value bytes
   * @param sourceOffset    where the value starts
   * @param sourceLength    the value's length in bytes
   * @param destination     where the codes are written; must hold {@link #maxCompressedLength} more
   *                        bytes at {@code destinationOffset}
   * @param destinationOffset where to write the codes
   * @return the number of bytes written
   */
  int compress(byte[] source, int sourceOffset, int sourceLength, byte[] destination, int destinationOffset);
}
