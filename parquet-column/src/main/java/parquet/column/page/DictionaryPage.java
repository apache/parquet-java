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

import static parquet.Preconditions.checkNotNull;

import java.io.IOException;

import parquet.Ints;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;

/**
 * Data for a dictionary page
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryPage extends Page {

  private final BytesInput bytes;
  private final int dictionarySize;
  private final Encoding encoding;

  /**
   * creates an uncompressed page
   * @param bytes the content of the page
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public DictionaryPage(BytesInput bytes, int dictionarySize, Encoding encoding) {
    this(bytes, (int)bytes.size(), dictionarySize, encoding); // TODO: fix sizes long or int
  }

  /**
   * creates a dictionary page
   * @param bytes the (possibly compressed) content of the page
   * @param uncompressedSize the size uncompressed
   * @param dictionarySize the value count in the dictionary
   * @param encoding the encoding used
   */
  public DictionaryPage(BytesInput bytes, int uncompressedSize, int dictionarySize, Encoding encoding) {
    super(Ints.checkedCast(bytes.size()), uncompressedSize);
    this.bytes = checkNotNull(bytes, "bytes");
    this.dictionarySize = dictionarySize;
    this.encoding = checkNotNull(encoding, "encoding");
  }

  public BytesInput getBytes() {
    return bytes;
  }

  public int getDictionarySize() {
    return dictionarySize;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public DictionaryPage copy() throws IOException {
    return new DictionaryPage(BytesInput.copy(bytes), getUncompressedSize(), dictionarySize, encoding);
  }


  @Override
  public String toString() {
    return "Page [bytes.size=" + bytes.size() + ", entryCount=" + dictionarySize + ", uncompressedSize=" + getUncompressedSize() + ", encoding=" + encoding + "]";
  }


}
