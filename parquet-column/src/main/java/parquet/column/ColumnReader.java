/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column;

import parquet.io.Binary;

/**
 * reader for (repetition level, definition level, values) triplets
 * each iteration looks at the current definition level and value as well as the next repetition level
 *
 * @author Julien Le Dem
 *
 */
public interface ColumnReader {

  /**
   * if the data has been fully consumed
   * @return
   */
  boolean isFullyConsumed();

  /**
   * moves to the next value
   */
  void consume();

  /**
   * must return 0 when isFullyConsumed() == true
   * @return the repetition level for the current value
   */
  int getCurrentRepetitionLevel();

  /**
   *
   * @return the definition level for the current value
   */
  int getCurrentDefinitionLevel();

  /**
   *
   * @return the current value
   */
  String getString();

  /**
   *
   * @return the current value
   */
  int getInteger();

  /**
   *
   * @return the current value
   */
  boolean getBoolean();

  /**
   *
   * @return the current value
   */
  long getLong();

  /**
   *
   * @return the current value
   */
  Binary getBinary();

  /**
   *
   * @return the current value
   */
  float getFloat();

  /**
   *
   * @return the current value
   */
  double getDouble();

}
