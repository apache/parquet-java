/**
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
package parquet.hadoop.codec.buffers;

import java.nio.ByteBuffer;

/**
 * Interface abstracts out the difference between reusing the same
 * byte buffer everytime, or freeing/reallocating the buffer as required
 * to save on memory overheads (at the cost some cpu overhead)
 */
public interface CodecByteBuffer {
  /**
   * We do not need the buffer for now, reset it or free it
   */
  void resetBuffer();

  /**
   * Explicitly free the buffer
   */
  void freeBuffer();

  /**
   * Get the underlying ByteBuffer
   * @return byteBuffer
   */
  ByteBuffer get();
}
