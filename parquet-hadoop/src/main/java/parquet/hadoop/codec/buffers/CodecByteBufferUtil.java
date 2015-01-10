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

import parquet.Log;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * Util class for manipulating bytebuffer s
 */
public abstract class CodecByteBufferUtil {
  private static final Log LOG = Log.getLog(CodecByteBufferUtil.class);

  private CodecByteBufferUtil() {};

  public static void freeOffHeapBuffer(ByteBuffer buff) {
    if ((buff != null) && (buff.isDirect())) {
      // even though nio.DirectBuffer is package private, it implements
      // a public interface (sun.nio.ch.DirectBuffer), so we can cast
      // our buffer to the interface type to retrieve the cleaner
      Cleaner cleaner = ((DirectBuffer) buff).cleaner();

      // Forces the immediate release of the off heap buffer
      if (cleaner != null) {
        cleaner.clean();
      } else {
        LOG.warn("Will have to wait until object is finalized to free off-heap buffer");
      }
    }
  }
}
