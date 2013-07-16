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
package parquet.column.page;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;

/**
 * one page in a chunk
 *
 * @author Julien Le Dem
 *
 */
public class DataPage extends Page {

  private final int valueCount;
  private final Encoding rlEncoding;
  private final Encoding dlEncoding;
  private final Encoding valuesEncoding;

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param compressedSize the compressed size of the page
   * @param uncompressedSize the uncompressed size of the page
   * @param rlEncoding the repetition level encoding for this page
   * @param dlEncoding the definition level encoding for this page
   * @param valuesEncoding the values encoding for this page
   * @param dlEncoding
   */
  public DataPage(BytesInput bytes, int valueCount, int compressedSize, int uncompressedSize, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) {
    super(bytes, compressedSize, uncompressedSize);
    this.valueCount = valueCount;
    this.rlEncoding = rlEncoding;
    this.dlEncoding = dlEncoding;
    this.valuesEncoding = valuesEncoding;
  }

  /**
   *
   * @return the number of values in that page
   */
  public int getValueCount() {
    return valueCount;
  }

  /**
   * @return the definition level encoding for this page
   */
  public Encoding getDlEncoding() {
    return dlEncoding;
  }

  /**
   * @return the repetition level encoding for this page
   */
  public Encoding getRlEncoding() {
    return rlEncoding;
  }

  /**
   * @return the values encoding for this page
   */
  public Encoding getValueEncoding() {
    return valuesEncoding;
  }

  @Override
  public String toString() {
    return "DataPage [bytes.size=" + getBytes().size() + ", valueCount=" + valueCount + ", uncompressedSize=" + getUncompressedSize() + "]";
  }

  @Override
  public void accept(PageVisitor visitor) {
    visitor.visit(this);
  }

}