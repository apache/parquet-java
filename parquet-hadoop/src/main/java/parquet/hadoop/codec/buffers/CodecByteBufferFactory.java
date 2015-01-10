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
package parquet.hadoop.codec.buffers;

/**
 * abstracts out the difference between reusing the same
 * byte buffer everytime, or freeing/reallocating the buffer as required
 * to save on memory overheads (at the cost some cpu overhead)
 */
public class CodecByteBufferFactory {
  public enum BufferReuseOption {
    ReuseOnReset,
    FreeOnReset
  };

  private final BufferReuseOption bufferReuseOption;

  public CodecByteBufferFactory(BufferReuseOption bufferReuseOption) {
    this.bufferReuseOption = bufferReuseOption;
  }

  public CodecByteBuffer create(int bufsize) {
    return bufferReuseOption == BufferReuseOption.ReuseOnReset ?
        new ReuseOnResetByteBuffer(bufsize) :
        new FreeOnResetByteBuffer(bufsize);
  }
}
