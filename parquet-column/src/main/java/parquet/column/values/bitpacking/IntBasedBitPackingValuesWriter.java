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
package parquet.column.values.bitpacking;

import static parquet.column.Encoding.BIT_PACKED;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;

import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;

public class IntBasedBitPackingValuesWriter extends ValuesWriter {

  private final int bitWidth;
  private final IntPacker packer;
  private final int packedArraySize;
  private final int[] inputBuffer = new int[32];
  private final List<int[]> bytes = new ArrayList<int[]>();
  private int bufferedCount = 0;
  private int[] packed;
  private int packedCount;

  private IntBasedBitPackingValuesWriter(int bitWidth) {
    super();
    this.bitWidth = bitWidth;
    this.packer = LemireBitPackingBE.getPacker(bitWidth);
    this.packedArraySize = bitWidth * 100000;
    this.initPacked();
  }

  private void initPacked() {
    this.packed = new int[packedArraySize]; // TODO: figure out a better number
    this.packedCount = 0;
  };

  @Override
  public void writeInteger(int v) {
    inputBuffer[bufferedCount] = v;
    ++ bufferedCount;
    if (bufferedCount == 32) {
      packer.pack32Values(inputBuffer, 0, packed, packedCount);
      bufferedCount = 0;
      packedCount += bitWidth;
      if (packedCount == packed.length) {
        bytes.add(packed);
        initPacked();
      }
    }
  }

  @Override
  public long getBufferedSize() {
    return (packedCount + bytes.size() * packedArraySize) * 4;
  }

  @Override
  public BytesInput getBytes() {
    // TODO: rework this I may lose all the benefit of working on ints right here
    int size = (packedCount + bytes.size() * packedArraySize) * 4;
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.order(ByteOrder.BIG_ENDIAN); // Yes
    final IntBuffer intBuffer = buffer.asIntBuffer();
    for (int[] array : bytes) {
      intBuffer.put(array);
    }
    intBuffer.put(packed, 0, packedCount);
    packedCount = 0;
    return BytesInput.from(buffer.array());
  }

  @Override
  public Encoding getEncoding() {
    return BIT_PACKED;
  }

  @Override
  public void reset() {
    bufferedCount = 0;
    initPacked();
  }

  @Override
  public long getAllocatedSize() {
    return packed.length * 4 + inputBuffer.length * 4;
  }


}
