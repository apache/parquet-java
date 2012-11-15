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

/**
 * Raw content of a Metadata block in the file
 *
 * @author Julien Le Dem
 *
 */
public class MetaDataBlock {

  private final String name;
  private final byte[] data;

  /**
   *
   * @param name name of the block (must be unique)
   * @param data data for the block
   */
  public MetaDataBlock(String name, byte[] data) {
    this.name = name;
    this.data = data;
  }

  /**
   *
   * @return name of the block
   */
  public String getName() {
    return name;
  }

  /**
   *
   * @return raw content
   */
  public byte[] getData() {
    return data;
  }

}
