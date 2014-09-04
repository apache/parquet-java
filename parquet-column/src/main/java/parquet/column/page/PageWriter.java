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

import java.io.IOException;

import parquet.bytes.ByteBufferAllocator;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.statistics.Statistics;

/**
 * a writer for all the pages of a given column chunk
 *
 * @author Julien Le Dem
 *
 */
public interface PageWriter {

  @Deprecated
  /**
   * writes a single page
   * @param bytesInput the bytes for the page
   * @param valueCount the number of values in that page
   * @param rlEncoding repetition level encoding
   * @param dlEncoding definition level encoding
   * @param valuesEncoding values encoding
   * @throws IOException
   */
  void writePage(BytesInput bytesInput, int valueCount, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException;

  /**
   * writes a single page
   * @param bytesInput the bytes for the page
   * @param valueCount the number of values in that page
   * @param statistics the statistics for that page
   * @param rlEncoding repetition level encoding
   * @param dlEncoding definition level encoding
   * @param valuesEncoding values encoding
   * @throws IOException
   */
  void writePage(BytesInput bytesInput, int valueCount, Statistics statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException;

  /**
   * @return the current size used in the memory buffer for that column chunk
   */
  long getMemSize();

  /**
   * @return the allocated size for the buffer ( > getMemSize() )
   */
  long allocatedSize();

  /**
   * writes a dictionary page
   * @param dictionaryPage the dictionary page containing the dictionary data
   */
  void writeDictionaryPage(DictionaryPage dictionaryPage) throws IOException;

  public abstract String memUsageString(String prefix);

  /**
   * Gets the associated ByteBuffer allocator. The allocator is passed in all the way down to
   * the Column ValuesWriter(s).
   * @return
   */
  ByteBufferAllocator getAllocator();

  /**
   * Reset the page writer. Reset/reallocate any resources.
   */
  public void reset();


  /**
   * Close the page writer. Free any resources.
   */
  public void close();

}
