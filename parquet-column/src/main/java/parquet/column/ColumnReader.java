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
 * Reader for (repetition level, definition level, values) triplets.
 * At any given point in time, a ColumnReader points to a single (r, d, v) triplet.
 * In order to move to the next triplet, call {@link #consume()}.
 *
 * Depending on the type and the encoding of the column only a subset of the get* methods are implemented.
 * Dictionary specific methods enable the upper layers to read the dictionary IDs without decoding the data.
 * In particular the Converter will decode the strings in the dictionary only once and iterate on the
 * dictionary IDs instead of the values.
 *
 * <ul>Each iteration looks at the current definition level and value as well as the next
 * repetition level:
 *  <li> The current definition level defines if the value is null.</li>
 *  <li> If the value is defined we can read it with the correct get*() method.</li>
 *  <li> Looking ahead to the next repetition determines what is the next column to read for in the FSA.</li>
 * </ul>
 * @author Julien Le Dem
  */
public interface ColumnReader {

  /**
   * @return the totalCount of values to be consumed
   */
  long getTotalValueCount();

  /**
   * Consume the current triplet, moving to the next value.
   */
  void consume();

  /**
   * must return 0 when isFullyConsumed() == true
   * @return the repetition level for the current value
   */
  int getCurrentRepetitionLevel();

  /**
   * @return the definition level for the current value
   */
  int getCurrentDefinitionLevel();

  /**
   * writes the current value to the converter
   */
  void writeCurrentValueToConverter();

  /**
   * Skip the current value
   */
  void skip();

  /**
   * available when the underlying encoding is dictionary based
   * @return the dictionary id for the current value
   */
  int getCurrentValueDictionaryID();

  /**
   * @return the current value
   */
  int getInteger();

  /**
   * @return the current value
   */
  boolean getBoolean();

  /**
   * @return the current value
   */
  long getLong();

  /**
   * @return the current value
   */
  Binary getBinary();

  /**
   * @return the current value
   */
  float getFloat();

  /**
   * @return the current value
   */
  double getDouble();

  /**
   * @return Descriptor of the column.
   */
  ColumnDescriptor getDescriptor();

}
