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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import parquet.bytes.BytesInput;

public class ByteBasedBitPackingEncoder {

  private static final int VALUES_WRITTEN_AT_A_TIME = 8;
  /** must be a multiple of VALUES_WRITTEN_AT_A_TIME */
  private static final int SLAB_SIZE = VALUES_WRITTEN_AT_A_TIME * 1024 * 1024;

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] input = new int[VALUES_WRITTEN_AT_A_TIME];
  private int inputSize;
  private byte[] packed;
  private int packedPosition;
  private final List<BytesInput> slabs = new ArrayList<BytesInput>();
  private int totalValues;

  public ByteBasedBitPackingEncoder(int bitWidth) {
    this.bitWidth = bitWidth;
    this.inputSize = 0;
    initPackedSlab();
    packer = ByteBitPacking.getPacker(bitWidth);
  }

  public void writeInt(int value) throws IOException {
    input[inputSize] = value;
    ++ inputSize;
    if (inputSize == VALUES_WRITTEN_AT_A_TIME) {
      pack();
      if (packedPosition == SLAB_SIZE) {
        slabs.add(BytesInput.from(packed));
        initPackedSlab();
      }
    }
  }

  private void pack() {
    packer.pack8Values(input, 0, packed, packedPosition);
    packedPosition += bitWidth;
    totalValues += inputSize;
    inputSize = 0;
  }

  private void initPackedSlab() {
    packed = new byte[SLAB_SIZE];
    packedPosition = 0;
  }

  public BytesInput toBytes() throws IOException {
//    int packedByteLength = packedPosition + (inputSize * bitWidth + 7) / 8;
    if (inputSize > 0) {
      for (int i = inputSize; i < input.length; i++) {
        input[i] = 0;
      }
      pack();
    }
    return BytesInput.fromSequence(
        BytesInput.fromSequence(slabs),
        BytesInput.from(packed, 0, packedPosition)
        );
  }

}
