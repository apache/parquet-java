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

import org.apache.parquet.bytes.BytesInput;

/**
 * A table mapping codes to the byte sequences they stand for, in the form that is written to a file
 * and read back from one.
 *
 * <p>This is the decode-side and serialization view of a table. It deliberately knows nothing about
 * how the table was chosen or how values are compressed against it, so that a variant with wider
 * codes or a differently built table is a new implementation here rather than a change to the
 * writer and reader above it.
 */
public interface SymbolTable {

  SymbolTableType type();

  /** Number of symbols, excluding the escape mechanism. */
  int symbolCount();

  /** Length in bytes of the symbol with this code. */
  int symbolLength(int code);

  /**
   * Appends the symbol with this code to {@code destination} at {@code position}.
   *
   * @return the number of bytes written
   */
  int copySymbol(int code, byte[] destination, int position);

  /** The serialized table body, as it appears in a file. */
  BytesInput serialize();

  /** A decoder for code streams written against this table. */
  CodeStreamDecoder decoder();
}
