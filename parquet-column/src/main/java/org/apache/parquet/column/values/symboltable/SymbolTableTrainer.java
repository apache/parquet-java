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
 * Chooses a symbol table for a set of values.
 *
 * <p>This is the seam that separates how a table is chosen from how it is written and used. A
 * different code width, or a different way of mining symbols out of the data, is a new
 * implementation of this interface and of {@link SymbolTable}, with nothing above them changed.
 */
public interface SymbolTableTrainer {

  SymbolTableType type();

  /**
   * Trains a table on the given values, which the trainer may read in any order and more than once.
   */
  TrainedSymbolTable train(ValueBuffer values);
}
