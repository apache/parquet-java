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
 * Where a writer hands off the symbol table it trained.
 *
 * <p>A symbol table belongs to a column chunk rather than to a page: every page of the chunk is
 * compressed against it, and it has to be readable before any of them. A values writer cannot write
 * anything outside its own page, so it publishes the table here instead and something above it
 * decides where the bytes go.
 *
 * <p>That indirection is the point. It is a test harness today, and a page written next to the
 * dictionary page once the format carries one, and neither choice reaches the writer.
 */
public interface SymbolTableSink {

  /**
   * Publishes the table a column chunk's pages are compressed against.
   *
   * <p>Called once per chunk, when the first page is compressed. A chunk that then abandons the
   * encoding — because the codes did not come out smaller than the values — leaves a table behind
   * that no page refers to, so whoever stores it should write it only if some page of the chunk was
   * actually written with the encoding.
   *
   * @param type the representation, which a reader needs in order to interpret the body
   * @param body the serialized table
   */
  void putSymbolTable(SymbolTableType type, BytesInput body);
}
