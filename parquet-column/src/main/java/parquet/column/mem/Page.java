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
package parquet.column.mem;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;

/**
 * one page in a chunk
 *
 * @author Julien Le Dem
 *
 */
public class Page {
  private static final boolean DEBUG = Log.DEBUG;
  private static final Log LOG = Log.getLog(Page.class);

  private static int nextId = 0;

  private final BytesInput bytes;
  private final int valueCount;
  private final int uncompressedSize;
  private final Encoding encoding;
  private final int id;

  /**
   * @param bytes the bytes for this page
   * @param valueCount count of values in this page
   * @param uncompressedSize the uncompressed size of the page
   * @param encoding the encoding for this page
   */
  public Page(BytesInput bytes, int valueCount, int uncompressedSize, Encoding encoding) {
    this.bytes = bytes;
    this.valueCount = valueCount;
    this.uncompressedSize = uncompressedSize;
    this.encoding = encoding;
    this.id = nextId ++;
    if (DEBUG) LOG.debug("new Page #"+id+" : " + bytes.size() + " bytes and " + valueCount + " records");
  }

  /**
   *
   * @return the bytes for the page
   */
  public BytesInput getBytes() {
    return bytes;
  }

  /**
   *
   * @return the number of values in that page
   */
  public int getValueCount() {
    return valueCount;
  }

  /**
   *
   * @return the uncompressed size of the page when the bytes are compressed
   */
  public int getUncompressedSize() {
    return uncompressedSize;
  }

  /**
   *
   * @return the encoding for this page
   */
  public Encoding getEncoding() {
    return encoding;
  }

  @Override
  public String toString() {
    return "Page [id: " + id + ", bytes.size=" + bytes.size() + ", valueCount=" + valueCount + ", uncompressedSize=" + uncompressedSize + ", encoding=" + encoding + "]";
  }

}