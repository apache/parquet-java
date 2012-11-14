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
package redelm.hadoop;

import java.util.Arrays;

public class ColumnData {

  private final String[] path;
  private byte[] repetitionLevels;
  private byte[] definitionLevels;
  private byte[] data;

  public ColumnData(String[] path,
      byte[] repetitionLevels, byte[] definitionLevels, byte[] data) {
    super();
    this.path = path;
    this.repetitionLevels = repetitionLevels;
    this.definitionLevels = definitionLevels;
    this.data = data;
  }

  public String[] getPath() {
    return path;
  }

  public byte[] getRepetitionLevels() {
    return repetitionLevels;
  }

  public byte[] getDefinitionLevels() {
    return definitionLevels;
  }

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
