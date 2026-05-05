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
package org.apache.parquet.column;

import org.apache.parquet.io.api.Binary;

/**
 * writer for (repetition level, definition level, values) triplets
 */
public interface ColumnWriter extends AutoCloseable {

  /**
   * writes the current value
   *
   * @param value           an int value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(int value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   *
   * @param value           a long value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(long value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   *
   * @param value           a boolean value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(boolean value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   *
   * @param value           a Binary value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(Binary value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   *
   * @param value           a float value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(float value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   *
   * @param value           a double value
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void write(double value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current null value
   *
   * @param repetitionLevel a repetition level
   * @param definitionLevel a definition level
   */
  void writeNull(int repetitionLevel, int definitionLevel);

  /**
   * Close the underlying store. This should be called when there are no
   * more data to be written.
   */
  @Override
  void close();

  /**
   * used to decide when to write a page or row group
   *
   * @return the number of bytes of memory used to buffer the current data
   */
  long getBufferedSizeInMemory();
}
