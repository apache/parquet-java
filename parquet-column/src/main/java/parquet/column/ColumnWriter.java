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
package parquet.column;

import parquet.io.api.Binary;

/**
 * writer for (repetition level, definition level, values) triplets
 *
 * @author Julien Le Dem
 *
 */
public interface ColumnWriter {

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void write(int value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void write(long value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void write(boolean value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void write(Binary value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void write(float value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current value
   * @param value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void write(double value, int repetitionLevel, int definitionLevel);

  /**
   * writes the current null value
   * @param repetitionLevel
   * @param definitionLevel
   */
  void writeNull(int repetitionLevel, int definitionLevel);

}

