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
package parquet.column.page;

/**
 * one data page in a chunk
 *
 * @author Julien Le Dem
 *
 */
abstract public class DataPage extends Page {

  private final int valueCount;

  DataPage(int compressedSize, int uncompressedSize, int valueCount) {
    super(compressedSize, uncompressedSize);
    this.valueCount = valueCount;
  }

  /**
   * @return the number of values in that page
   */
  public int getValueCount() {
    return valueCount;
  }

  public abstract <T> T accept(Visitor<T> visitor);

  public static interface Visitor<T> {

    T visit(DataPageV1 dataPageV1);

    T visit(DataPageV2 dataPageV2);

  }

}