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
 * The representation of a symbol table, which fixes the width of a code in the code stream and the
 * framing of an escaped literal.
 *
 * <p>A column's encoding does not identify the representation on its own: a single encoding covers
 * every variant, and the type is carried with the table. A reader therefore has to read the table
 * before it can commit to decoding the column, and may reject a type it does not implement.
 *
 * <p>The numeric values are not ratified. Only {@link #FSST_8} appears in the format proposal so
 * far; {@link #FSST_16}'s value is this implementation's proposal and must be confirmed against
 * parquet-format issue #531 before any file written with it is treated as portable.
 */
public enum SymbolTableType {
  /** Single-byte codes, at most 255 symbols, escape marker 255 followed by one literal byte. */
  FSST_8(0, 1),

  /** Two-byte codes, at most 65535 symbols, escape marker 65535 followed by a literal. */
  FSST_16(1, 2);

  private final int typeValue;
  private final int codeWidth;

  SymbolTableType(int typeValue, int codeWidth) {
    this.typeValue = typeValue;
    this.codeWidth = codeWidth;
  }

  /** The value written to the symbol table page header. */
  public int typeValue() {
    return typeValue;
  }

  /** Width in bytes of one code in the code stream. */
  public int codeWidth() {
    return codeWidth;
  }

  public static SymbolTableType fromTypeValue(int typeValue) {
    for (SymbolTableType type : values()) {
      if (type.typeValue == typeValue) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unsupported symbol table type: " + typeValue);
  }
}
