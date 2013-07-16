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

/**
 * a writer for all the pages of a given column chunk
 *
 * @author Julien Le Dem
 *
 */
public interface PageWriter {

  /**
   * writes a single page
   * @param dataPage the page to write
   * @throws IOException
   */
  void writeDataPage(DataPage dataPage) throws IOException;

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
   * generates a description of memory usage for display
   * @param prefix the prefix to use for display
   * @return a string to display how memory is used
   */
  String memUsageString(String prefix);

}
