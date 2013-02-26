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
package parquet.bytes;

import java.io.ByteArrayOutputStream;

/**
 * expose the memory used by a ByteArrayOutputStream
 *
 * @author Julien Le Dem
 *
 */
public class CapacityByteArrayOutputStream extends ByteArrayOutputStream {

  public CapacityByteArrayOutputStream(int initialSize) {
    super(initialSize);
  }

  /**
   *
   * @return the size of the allocated buffer
   */
  public int getCapacity() {
    return buf.length;
  }

}