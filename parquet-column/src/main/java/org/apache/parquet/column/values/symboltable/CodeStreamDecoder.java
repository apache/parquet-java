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
 * Expands a stream of codes back into the values it was made from.
 *
 * <p>The decoder works on one value's slice of the code stream, which is what makes a value
 * addressable without expanding the ones before it.
 */
public interface CodeStreamDecoder {

  /**
   * How many bytes one value's codes expand to, so a caller can size a destination buffer.
   *
   * <p>Cheaper than expanding, because it reads the codes without copying symbol bytes.
   */
  int expandedLength(byte[] codes, int codesOffset, int codesLength);

  /**
   * Expands one value's codes.
   *
   * @param codes        the code stream
   * @param codesOffset  where this value's codes start
   * @param codesLength  how many bytes of codes belong to this value
   * @param destination  where the value bytes are written
   * @param destinationOffset where to write the value bytes
   * @return the number of bytes written
   */
  int expand(byte[] codes, int codesOffset, int codesLength, byte[] destination, int destinationOffset);
}
