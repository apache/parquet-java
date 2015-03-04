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

import java.io.IOException;

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
  void writePage(BytesInput bytesInput, int valueCount, Statistics<?> statistics, Encoding rlEncoding, Encoding dlEncoding, Encoding valuesEncoding) throws IOException;

  /**
   * writes a single page in the new format
   * @param rowCount the number of rows in this page
   * @param nullCount the number of null values (out of valueCount)
   * @param valueCount the number of values in that page (there could be multiple values per row for repeated fields)
   * @param repetitionLevels the repetition levels encoded in RLE without any size header
   * @param definitionLevels the definition levels encoded in RLE without any size header
   * @param dataEncoding the encoding for the data
   * @param data the data encoded with dataEncoding
   * @param statistics optional stats for this page
   * @param metadata optional free form key values
   * @throws IOException
   */
  void writePageV2(
      int rowCount, int nullCount, int valueCount,
      BytesInput repetitionLevels, BytesInput definitionLevels,
      Encoding dataEncoding,
      BytesInput data,
      Statistics<?> statistics) throws IOException;

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

  /**
   * @param prefix a prefix header to add at every line
   * @return a string presenting a summary of how memory is used
   */
  String memUsageString(String prefix);

}
