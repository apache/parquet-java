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
 * Where a reader gets the symbol table a column chunk's pages were compressed against.
 *
 * <p>The counterpart of {@link SymbolTableSink}. The table arrives already deserialized, because
 * which implementation to build from the bytes depends on the representation and that decision
 * belongs in one place: {@link SymbolTables}.
 */
public interface SymbolTableSource {

  /**
   * The table for the chunk being read.
   *
   * <p>Called before the first page. Implementations are expected to deserialize once and hand back
   * the same table for every page of the chunk.
   */
  SymbolTable getSymbolTable();
}
