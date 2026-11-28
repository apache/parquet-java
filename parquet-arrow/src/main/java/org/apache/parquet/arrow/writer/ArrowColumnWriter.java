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
package org.apache.parquet.arrow.writer;

import java.io.IOException;
import org.apache.arrow.vector.FieldVector;

/**
 * Strategy interface for writing Arrow column data to a Parquet page.
 *
 * <p>Implementations are selected once per column based on the column's type,
 * nullability, and encoding configuration. Each implementation represents
 * the most efficient write path for that combination.
 */
public interface ArrowColumnWriter {

  /**
   * Writes values from the given Arrow vector (rows {@code offset} to {@code offset + length - 1})
   * to the underlying Parquet page writer.
   *
   * @param vector the Arrow vector containing column values
   * @param offset the first row index to write (inclusive)
   * @param length the number of rows to write
   * @throws IOException if an I/O error occurs during page writing
   */
  void write(FieldVector vector, int offset, int length) throws IOException;
}
