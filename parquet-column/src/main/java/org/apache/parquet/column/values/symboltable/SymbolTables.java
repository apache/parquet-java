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

import org.apache.parquet.column.values.symboltable.fsst.Fsst8SymbolTable;
import org.apache.parquet.column.values.symboltable.fsst.FsstTrainer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * The one place that maps a symbol table representation to the code that implements it.
 *
 * <p>Both directions live here so that a new representation is a new implementation plus two lines
 * of dispatch, and so that a reader's rejection of a representation it does not implement happens in
 * one place with one message.
 */
public final class SymbolTables {

  private SymbolTables() {}

  /** A trainer producing tables of this representation. */
  public static SymbolTableTrainer trainer(SymbolTableType type) {
    switch (type) {
      case FSST_8:
        return new FsstTrainer();
      default:
        throw new IllegalArgumentException("No symbol table trainer for " + type);
    }
  }

  /**
   * Rebuilds a table from a serialized body.
   *
   * @throws ParquetDecodingException if this implementation cannot read the representation, which a
   *     reader is allowed to do and is not the same as the file being corrupt
   */
  public static SymbolTable deserialize(SymbolTableType type, byte[] body, int offset, int length) {
    switch (type) {
      case FSST_8:
        return Fsst8SymbolTable.deserialize(body, offset, length);
      default:
        throw new ParquetDecodingException("Unsupported symbol table type: " + type);
    }
  }
}
