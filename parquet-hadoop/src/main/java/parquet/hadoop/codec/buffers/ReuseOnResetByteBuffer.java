package parquet.hadoop.codec.buffers;
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

import java.nio.ByteBuffer;

/**
 * Wrapper around DirectBuffer that implements the default original
 * behaviour of keeping the bytebuffer allocated even when
 * we are finished using it so we can reuse it when needed
 */
public class ReuseOnResetByteBuffer implements CodecByteBuffer {
  private ByteBuffer buf;
  private int buffsize;

  public ReuseOnResetByteBuffer(int buffsize) {
    buf = ByteBuffer.allocateDirect(buffsize);
    this.buffsize = buffsize;
  }

  public ByteBuffer get() { return buf; }

  public void resetBuffer() {
    buf.rewind();
    buf.limit(0);
  }

  public void freeBuffer() {
    CodecByteBufferUtil.freeOffHeapBuffer(buf);
    buf = null;
  }
}
