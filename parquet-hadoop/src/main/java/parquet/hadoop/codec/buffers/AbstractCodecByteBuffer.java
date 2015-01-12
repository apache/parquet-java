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
 * Base class that implements common functionality around freeing and
 * allocating bytebuffers
 */
public abstract class AbstractCodecByteBuffer implements CodecByteBuffer {

  final public int buffsize;

  protected ByteBuffer buf = null;

  public boolean hasRemaining() {
    return (buf != null) && (buf.hasRemaining());
  }

  /**
   * Explicitly free the off-heap buffer
   */
  public void freeBuffer() {
    if (buf != null) {
      CodecByteBufferUtil.freeOffHeapBuffer(buf);
      // The rest will be cleaned up when the buffer object is finalized
      buf = null;
    }
  }

  /**
   * Allocates a direct byte buffer of the size passed to the constructuor
   */
  protected void allocateBuffer()
  {
    buf = ByteBuffer.allocateDirect(buffsize);
  }

  protected AbstractCodecByteBuffer(final int buffsize) {
    this.buffsize = buffsize;
  }
}
