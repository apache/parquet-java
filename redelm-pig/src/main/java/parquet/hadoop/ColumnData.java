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
package parquet.hadoop;

import java.util.Arrays;

/**
 * Column data for a given block in raw format
 *
 * @author Julien Le Dem
 *
 */
public class ColumnData {

  private final String[] path;
  private final byte[] repetitionLevels;
  private final byte[] definitionLevels;
  private final byte[] data;

  /**
   *
   * @param path identifier of the column
   * @param repetitionLevels repetition levels data
   * @param definitionLevels definition levels data
   * @param data actual column data
   */
  public ColumnData(String[] path,
      byte[] repetitionLevels, byte[] definitionLevels, byte[] data) {
    super();
    this.path = path;
    this.repetitionLevels = repetitionLevels;
    this.definitionLevels = definitionLevels;
    this.data = data;
  }

  /**
   *
   * @return identifier of the column
   */
  public String[] getPath() {
    return path;
  }

  /**
   *
   * @return repetition level data
   */
  public byte[] getRepetitionLevels() {
    return repetitionLevels;
  }

  /**
   *
   * @return definition levels data
   */
  public byte[] getDefinitionLevels() {
    return definitionLevels;
  }

  /**
   *
   * @return raw column data
   */
  public byte[] getData() {
    return data;
  }

  @Override
  public String toString() {
    return "ColumnData{"+Arrays.toString(path) + " "
        +  repetitionLevels.length + "B "
        +  data.length + "B "
        +  data.length + "B}";
  }
}
