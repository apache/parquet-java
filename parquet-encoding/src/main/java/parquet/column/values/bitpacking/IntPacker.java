/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.column.values.bitpacking;

/**
 * Packs and unpacks into ints
 *
 * packing unpacking treats:
 *  - 32 values at a time
 *  - bitWidth ints at a time.
 *
 * @author Julien Le Dem
 *
 */
public abstract class IntPacker {

  private final int bitWidth;

  IntPacker(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  /**
   * @return the width in bits used for encoding, also how many ints are packed/unpacked at a time
   */
  public final int getBitWidth() {
    return bitWidth;
  }

  /**
   * pack 32 values from input at inPos into bitWidth ints in output at outPos.
   * nextPosition: inPos += 32; outPos += getBitWidth()
   * @param input the input values
   * @param inPos where to read from in input
   * @param output the output ints
   * @param outPos where to write to in output
   */
  public abstract void pack32Values(int[] input, int inPos, int[] output, int outPos);

  /**
   * unpack bitWidth ints from input at inPos into 32 values in output at outPos.
   * nextPosition: inPos += getBitWidth(); outPos += 32
   * @param input the input int
   * @param inPos where to read from in input
   * @param output the output values
   * @param outPos where to write to in output
   */
  public abstract void unpack32Values(int[] input, int inPos, int[] output, int outPos);

}
