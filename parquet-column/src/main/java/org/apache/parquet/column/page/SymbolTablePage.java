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

import java.io.IOException;
import java.util.Objects;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.values.symboltable.SymbolTable;
import org.apache.parquet.column.values.symboltable.SymbolTableType;
import org.apache.parquet.column.values.symboltable.SymbolTables;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * Data for a symbol table page.
 *
 * <p>A symbol table belongs to a column chunk the way a dictionary does: every page of the chunk is
 * compressed against it, and it has to be readable before any of them. This page is that page's
 * mirror, one per chunk instead of one per row of a data page.
 */
public class SymbolTablePage extends Page {

  private final BytesInput bytes;
  private final SymbolTableType type;

  /**
   * creates an uncompressed page
   *
   * @param bytes the content of the page
   * @param type  the symbol table representation
   */
  public SymbolTablePage(BytesInput bytes, SymbolTableType type) {
    this(bytes, (int) bytes.size(), type);
  }

  /**
   * creates a symbol table page
   *
   * @param bytes            the (possibly compressed) content of the page
   * @param uncompressedSize the size uncompressed
   * @param type             the symbol table representation
   */
  public SymbolTablePage(BytesInput bytes, int uncompressedSize, SymbolTableType type) {
    super(Math.toIntExact(bytes.size()), uncompressedSize);
    this.bytes = Objects.requireNonNull(bytes, "bytes cannot be null");
    this.type = Objects.requireNonNull(type, "type cannot be null");
  }

  public BytesInput getBytes() {
    return bytes;
  }

  public SymbolTableType getType() {
    return type;
  }

  public SymbolTablePage copy() throws IOException {
    return new SymbolTablePage(BytesInput.copy(bytes), getUncompressedSize(), type);
  }

  /**
   * @return the decoded symbol table
   */
  public SymbolTable decode() {
    try {
      return SymbolTables.deserialize(type, bytes.toByteArray(), 0, (int) bytes.size());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not decode the symbol table", e);
    }
  }

  @Override
  public String toString() {
    return "SymbolTablePage [bytes.size=" + bytes.size() + ", type=" + type + ", uncompressedSize="
        + getUncompressedSize() + "]";
  }
}
