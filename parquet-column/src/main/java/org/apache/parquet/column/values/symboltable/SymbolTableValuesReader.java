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

import java.io.IOException;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

/**
 * Reads the pages {@link SymbolTableValuesWriter} writes.
 *
 * <p>One reader serves every symbol table representation, for the same reason one writer does: the
 * page body is the same shape whatever the table holds, and expanding a value's codes is the table's
 * own business. Which representation a chunk used is carried with the table rather than by the
 * column's encoding, so this reader learns it from the {@link SymbolTableSource} and never has to
 * guess.
 *
 * <p>A value is addressable without expanding the ones before it, because the page records where
 * each value's codes end. That is what makes {@link #skip} cost nothing and is the property the
 * encoding exists for.
 */
public class SymbolTableValuesReader extends ValuesReader {

  private final SymbolTableSource source;

  private SymbolTable table;
  private CodeStreamDecoder decoder;
  private SymbolTablePayload payload;
  private int index;

  /**
   * A reader with nowhere to get its table from, which cannot decode a page.
   *
   * <p>The format does not carry a symbol table yet, so there is no place for a page reader to find
   * one and nothing to hand this constructor. It exists so that the encoding is complete on the read
   * side up to that one missing piece; supply a source and the reader works. See parquet-format issue
   * #531.
   */
  public SymbolTableValuesReader() {
    this(null);
  }

  public SymbolTableValuesReader(SymbolTableSource source) {
    this.source = source;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    if (table == null) {
      if (source == null) {
        throw new ParquetDecodingException(
            "Cannot decode a symbol table encoded page without the chunk's symbol table, which the "
                + "format does not carry yet: see parquet-format issue #531");
      }
      // Once per chunk. The table outlives the page: every page of the chunk shares it.
      table = source.getSymbolTable();
      decoder = table.decoder();
    }
    payload = SymbolTablePayload.parse(in, valueCount);
    index = 0;
  }

  /** The representation of the table this reader is decoding against. */
  public SymbolTableType symbolTableType() {
    return table == null ? null : table.type();
  }

  @Override
  public Binary readBytes() {
    checkHasValue(1);
    byte[] codes = payload.codes();
    int offset = payload.codeStart(index);
    int length = payload.codeLength(index);
    index++;
    byte[] value = new byte[decoder.expandedLength(codes, offset, length)];
    decoder.expand(codes, offset, length, value, 0);
    return Binary.fromConstantByteArray(value);
  }

  @Override
  public void skip() {
    skip(1);
  }

  @Override
  public void skip(int n) {
    checkHasValue(n);
    index += n;
  }

  private void checkHasValue(int n) {
    if (payload == null) {
      throw new ParquetDecodingException("Symbol table reader used before a page was read");
    }
    if (index + n > payload.valueCount()) {
      throw new ParquetDecodingException("Read past the end of a symbol table page: asked for value "
          + (index + n) + " of " + payload.valueCount());
    }
  }
}
