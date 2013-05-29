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

/**
 * Factory for packing implementations
 *
 * @author Julien Le Dem
 *
 */
public enum Packer {

  /**
   * packers who fill the Least Significant Bit First
   * int and byte packer have the same result on Big Endian architectures
   */
  BIG_ENDIAN {
    @Override
    IntPacker newIntPacker(int width) {
      return LemireBitPackingBE.getPacker(width);
    }
    @Override
    BytePacker newBytePacker(int width) {
      return ByteBitPackingBE.getPacker(width);
    }
  },

  /**
   * packers who fill the Most Significant Bit first
   * int and byte packer have the same result on Little Endian architectures
   */
  LITTLE_ENDIAN {
    @Override
    IntPacker newIntPacker(int width) {
      return LemireBitPackingLE.getPacker(width);
    }
    @Override
    BytePacker newBytePacker(int width) {
      return ByteBitPackingLE.getPacker(width);
    }
  };

  /**
   * @param width the width in bits of the packed values
   * @return an int based packer
   */
  abstract IntPacker newIntPacker(int width);

  /**
   * @param width the width in bits of the packed values
   * @return a byte based packer
   */
  abstract BytePacker newBytePacker(int width);
}