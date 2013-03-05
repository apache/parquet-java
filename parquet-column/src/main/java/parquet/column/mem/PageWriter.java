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

import java.io.IOException;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;

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
   * @throws IOException
   */
  abstract public void writePage(BytesInput bytesInput, int valueCount, Encoding encoding) throws IOException;

  /**
   *
   * @return the current size used in the memory buffer for that column chunk
   */
  abstract public long getMemSize();

  /**
   *
   * @return the allocated size for the buffer ( > getMemSize() )
   */
  public abstract long allocatedSize();

}
