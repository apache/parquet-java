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
/**
 * This is code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * (c) Daniel Lemire, http://lemire.me/en/
 */
package parquet.column.values.bitpacking;

/**
 * Scheme designed by D. Lemire
 * 
 * @author automatically generated
 * @see BitPackingGenerator
 *
 */
abstract class LemireBitPackingLE {

  private static final IntPacker[] packers = new IntPacker[32];
  static {
    packers[0] = new Packer0();
    packers[1] = new Packer1();
    packers[2] = new Packer2();
    packers[3] = new Packer3();
    packers[4] = new Packer4();
    packers[5] = new Packer5();
    packers[6] = new Packer6();
    packers[7] = new Packer7();
    packers[8] = new Packer8();
    packers[9] = new Packer9();
    packers[10] = new Packer10();
    packers[11] = new Packer11();
    packers[12] = new Packer12();
    packers[13] = new Packer13();
    packers[14] = new Packer14();
    packers[15] = new Packer15();
    packers[16] = new Packer16();
    packers[17] = new Packer17();
    packers[18] = new Packer18();
    packers[19] = new Packer19();
    packers[20] = new Packer20();
    packers[21] = new Packer21();
    packers[22] = new Packer22();
    packers[23] = new Packer23();
    packers[24] = new Packer24();
    packers[25] = new Packer25();
    packers[26] = new Packer26();
    packers[27] = new Packer27();
    packers[28] = new Packer28();
    packers[29] = new Packer29();
    packers[30] = new Packer30();
    packers[31] = new Packer31();
  }

  public static final IntPacker getPacker(int bitWidth) {
    return packers[bitWidth];
  }

  private static final class Packer0 extends IntPacker {

    private Packer0() {
      super(0);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
    }
  }

  private static final class Packer1 extends IntPacker {

    private Packer1() {
      super(1);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 1) <<   0)
        | ((in[ 1 + inPos] & 1) <<   1)
        | ((in[ 2 + inPos] & 1) <<   2)
        | ((in[ 3 + inPos] & 1) <<   3)
        | ((in[ 4 + inPos] & 1) <<   4)
        | ((in[ 5 + inPos] & 1) <<   5)
        | ((in[ 6 + inPos] & 1) <<   6)
        | ((in[ 7 + inPos] & 1) <<   7)
        | ((in[ 8 + inPos] & 1) <<   8)
        | ((in[ 9 + inPos] & 1) <<   9)
        | ((in[10 + inPos] & 1) <<  10)
        | ((in[11 + inPos] & 1) <<  11)
        | ((in[12 + inPos] & 1) <<  12)
        | ((in[13 + inPos] & 1) <<  13)
        | ((in[14 + inPos] & 1) <<  14)
        | ((in[15 + inPos] & 1) <<  15)
        | ((in[16 + inPos] & 1) <<  16)
        | ((in[17 + inPos] & 1) <<  17)
        | ((in[18 + inPos] & 1) <<  18)
        | ((in[19 + inPos] & 1) <<  19)
        | ((in[20 + inPos] & 1) <<  20)
        | ((in[21 + inPos] & 1) <<  21)
        | ((in[22 + inPos] & 1) <<  22)
        | ((in[23 + inPos] & 1) <<  23)
        | ((in[24 + inPos] & 1) <<  24)
        | ((in[25 + inPos] & 1) <<  25)
        | ((in[26 + inPos] & 1) <<  26)
        | ((in[27 + inPos] & 1) <<  27)
        | ((in[28 + inPos] & 1) <<  28)
        | ((in[29 + inPos] & 1) <<  29)
        | ((in[30 + inPos] & 1) <<  30)
        | ((in[31 + inPos] & 1) <<  31);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 1);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  1) & 1);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>>  2) & 1);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>>  3) & 1);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>>  4) & 1);
      out[ 5 + outPos] = ((in[ 0 + inPos] >>>  5) & 1);
      out[ 6 + outPos] = ((in[ 0 + inPos] >>>  6) & 1);
      out[ 7 + outPos] = ((in[ 0 + inPos] >>>  7) & 1);
      out[ 8 + outPos] = ((in[ 0 + inPos] >>>  8) & 1);
      out[ 9 + outPos] = ((in[ 0 + inPos] >>>  9) & 1);
      out[10 + outPos] = ((in[ 0 + inPos] >>> 10) & 1);
      out[11 + outPos] = ((in[ 0 + inPos] >>> 11) & 1);
      out[12 + outPos] = ((in[ 0 + inPos] >>> 12) & 1);
      out[13 + outPos] = ((in[ 0 + inPos] >>> 13) & 1);
      out[14 + outPos] = ((in[ 0 + inPos] >>> 14) & 1);
      out[15 + outPos] = ((in[ 0 + inPos] >>> 15) & 1);
      out[16 + outPos] = ((in[ 0 + inPos] >>> 16) & 1);
      out[17 + outPos] = ((in[ 0 + inPos] >>> 17) & 1);
      out[18 + outPos] = ((in[ 0 + inPos] >>> 18) & 1);
      out[19 + outPos] = ((in[ 0 + inPos] >>> 19) & 1);
      out[20 + outPos] = ((in[ 0 + inPos] >>> 20) & 1);
      out[21 + outPos] = ((in[ 0 + inPos] >>> 21) & 1);
      out[22 + outPos] = ((in[ 0 + inPos] >>> 22) & 1);
      out[23 + outPos] = ((in[ 0 + inPos] >>> 23) & 1);
      out[24 + outPos] = ((in[ 0 + inPos] >>> 24) & 1);
      out[25 + outPos] = ((in[ 0 + inPos] >>> 25) & 1);
      out[26 + outPos] = ((in[ 0 + inPos] >>> 26) & 1);
      out[27 + outPos] = ((in[ 0 + inPos] >>> 27) & 1);
      out[28 + outPos] = ((in[ 0 + inPos] >>> 28) & 1);
      out[29 + outPos] = ((in[ 0 + inPos] >>> 29) & 1);
      out[30 + outPos] = ((in[ 0 + inPos] >>> 30) & 1);
      out[31 + outPos] = ((in[ 0 + inPos] >>> 31) & 1);
    }
  }

  private static final class Packer2 extends IntPacker {

    private Packer2() {
      super(2);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 3) <<   0)
        | ((in[ 1 + inPos] & 3) <<   2)
        | ((in[ 2 + inPos] & 3) <<   4)
        | ((in[ 3 + inPos] & 3) <<   6)
        | ((in[ 4 + inPos] & 3) <<   8)
        | ((in[ 5 + inPos] & 3) <<  10)
        | ((in[ 6 + inPos] & 3) <<  12)
        | ((in[ 7 + inPos] & 3) <<  14)
        | ((in[ 8 + inPos] & 3) <<  16)
        | ((in[ 9 + inPos] & 3) <<  18)
        | ((in[10 + inPos] & 3) <<  20)
        | ((in[11 + inPos] & 3) <<  22)
        | ((in[12 + inPos] & 3) <<  24)
        | ((in[13 + inPos] & 3) <<  26)
        | ((in[14 + inPos] & 3) <<  28)
        | ((in[15 + inPos] & 3) <<  30);
      out[ 1 + outPos] =
          ((in[16 + inPos] & 3) <<   0)
        | ((in[17 + inPos] & 3) <<   2)
        | ((in[18 + inPos] & 3) <<   4)
        | ((in[19 + inPos] & 3) <<   6)
        | ((in[20 + inPos] & 3) <<   8)
        | ((in[21 + inPos] & 3) <<  10)
        | ((in[22 + inPos] & 3) <<  12)
        | ((in[23 + inPos] & 3) <<  14)
        | ((in[24 + inPos] & 3) <<  16)
        | ((in[25 + inPos] & 3) <<  18)
        | ((in[26 + inPos] & 3) <<  20)
        | ((in[27 + inPos] & 3) <<  22)
        | ((in[28 + inPos] & 3) <<  24)
        | ((in[29 + inPos] & 3) <<  26)
        | ((in[30 + inPos] & 3) <<  28)
        | ((in[31 + inPos] & 3) <<  30);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 3);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  2) & 3);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>>  4) & 3);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>>  6) & 3);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>>  8) & 3);
      out[ 5 + outPos] = ((in[ 0 + inPos] >>> 10) & 3);
      out[ 6 + outPos] = ((in[ 0 + inPos] >>> 12) & 3);
      out[ 7 + outPos] = ((in[ 0 + inPos] >>> 14) & 3);
      out[ 8 + outPos] = ((in[ 0 + inPos] >>> 16) & 3);
      out[ 9 + outPos] = ((in[ 0 + inPos] >>> 18) & 3);
      out[10 + outPos] = ((in[ 0 + inPos] >>> 20) & 3);
      out[11 + outPos] = ((in[ 0 + inPos] >>> 22) & 3);
      out[12 + outPos] = ((in[ 0 + inPos] >>> 24) & 3);
      out[13 + outPos] = ((in[ 0 + inPos] >>> 26) & 3);
      out[14 + outPos] = ((in[ 0 + inPos] >>> 28) & 3);
      out[15 + outPos] = ((in[ 0 + inPos] >>> 30) & 3);
      out[16 + outPos] = ((in[ 1 + inPos] >>>  0) & 3);
      out[17 + outPos] = ((in[ 1 + inPos] >>>  2) & 3);
      out[18 + outPos] = ((in[ 1 + inPos] >>>  4) & 3);
      out[19 + outPos] = ((in[ 1 + inPos] >>>  6) & 3);
      out[20 + outPos] = ((in[ 1 + inPos] >>>  8) & 3);
      out[21 + outPos] = ((in[ 1 + inPos] >>> 10) & 3);
      out[22 + outPos] = ((in[ 1 + inPos] >>> 12) & 3);
      out[23 + outPos] = ((in[ 1 + inPos] >>> 14) & 3);
      out[24 + outPos] = ((in[ 1 + inPos] >>> 16) & 3);
      out[25 + outPos] = ((in[ 1 + inPos] >>> 18) & 3);
      out[26 + outPos] = ((in[ 1 + inPos] >>> 20) & 3);
      out[27 + outPos] = ((in[ 1 + inPos] >>> 22) & 3);
      out[28 + outPos] = ((in[ 1 + inPos] >>> 24) & 3);
      out[29 + outPos] = ((in[ 1 + inPos] >>> 26) & 3);
      out[30 + outPos] = ((in[ 1 + inPos] >>> 28) & 3);
      out[31 + outPos] = ((in[ 1 + inPos] >>> 30) & 3);
    }
  }

  private static final class Packer3 extends IntPacker {

    private Packer3() {
      super(3);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 7) <<   0)
        | ((in[ 1 + inPos] & 7) <<   3)
        | ((in[ 2 + inPos] & 7) <<   6)
        | ((in[ 3 + inPos] & 7) <<   9)
        | ((in[ 4 + inPos] & 7) <<  12)
        | ((in[ 5 + inPos] & 7) <<  15)
        | ((in[ 6 + inPos] & 7) <<  18)
        | ((in[ 7 + inPos] & 7) <<  21)
        | ((in[ 8 + inPos] & 7) <<  24)
        | ((in[ 9 + inPos] & 7) <<  27)
        | ((in[10 + inPos] & 7) <<  30);
      out[ 1 + outPos] =
          ((in[10 + inPos] & 7) >>>  2)
        | ((in[11 + inPos] & 7) <<   1)
        | ((in[12 + inPos] & 7) <<   4)
        | ((in[13 + inPos] & 7) <<   7)
        | ((in[14 + inPos] & 7) <<  10)
        | ((in[15 + inPos] & 7) <<  13)
        | ((in[16 + inPos] & 7) <<  16)
        | ((in[17 + inPos] & 7) <<  19)
        | ((in[18 + inPos] & 7) <<  22)
        | ((in[19 + inPos] & 7) <<  25)
        | ((in[20 + inPos] & 7) <<  28)
        | ((in[21 + inPos] & 7) <<  31);
      out[ 2 + outPos] =
          ((in[21 + inPos] & 7) >>>  1)
        | ((in[22 + inPos] & 7) <<   2)
        | ((in[23 + inPos] & 7) <<   5)
        | ((in[24 + inPos] & 7) <<   8)
        | ((in[25 + inPos] & 7) <<  11)
        | ((in[26 + inPos] & 7) <<  14)
        | ((in[27 + inPos] & 7) <<  17)
        | ((in[28 + inPos] & 7) <<  20)
        | ((in[29 + inPos] & 7) <<  23)
        | ((in[30 + inPos] & 7) <<  26)
        | ((in[31 + inPos] & 7) <<  29);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 7);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  3) & 7);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>>  6) & 7);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>>  9) & 7);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>> 12) & 7);
      out[ 5 + outPos] = ((in[ 0 + inPos] >>> 15) & 7);
      out[ 6 + outPos] = ((in[ 0 + inPos] >>> 18) & 7);
      out[ 7 + outPos] = ((in[ 0 + inPos] >>> 21) & 7);
      out[ 8 + outPos] = ((in[ 0 + inPos] >>> 24) & 7);
      out[ 9 + outPos] = ((in[ 0 + inPos] >>> 27) & 7);
      out[10 + outPos] = ((in[ 0 + inPos] >>> 30) & 7) | ((in[ 1 + inPos] & 1) <<  2);
      out[11 + outPos] = ((in[ 1 + inPos] >>>  1) & 7);
      out[12 + outPos] = ((in[ 1 + inPos] >>>  4) & 7);
      out[13 + outPos] = ((in[ 1 + inPos] >>>  7) & 7);
      out[14 + outPos] = ((in[ 1 + inPos] >>> 10) & 7);
      out[15 + outPos] = ((in[ 1 + inPos] >>> 13) & 7);
      out[16 + outPos] = ((in[ 1 + inPos] >>> 16) & 7);
      out[17 + outPos] = ((in[ 1 + inPos] >>> 19) & 7);
      out[18 + outPos] = ((in[ 1 + inPos] >>> 22) & 7);
      out[19 + outPos] = ((in[ 1 + inPos] >>> 25) & 7);
      out[20 + outPos] = ((in[ 1 + inPos] >>> 28) & 7);
      out[21 + outPos] = ((in[ 1 + inPos] >>> 31) & 7) | ((in[ 2 + inPos] & 3) <<  1);
      out[22 + outPos] = ((in[ 2 + inPos] >>>  2) & 7);
      out[23 + outPos] = ((in[ 2 + inPos] >>>  5) & 7);
      out[24 + outPos] = ((in[ 2 + inPos] >>>  8) & 7);
      out[25 + outPos] = ((in[ 2 + inPos] >>> 11) & 7);
      out[26 + outPos] = ((in[ 2 + inPos] >>> 14) & 7);
      out[27 + outPos] = ((in[ 2 + inPos] >>> 17) & 7);
      out[28 + outPos] = ((in[ 2 + inPos] >>> 20) & 7);
      out[29 + outPos] = ((in[ 2 + inPos] >>> 23) & 7);
      out[30 + outPos] = ((in[ 2 + inPos] >>> 26) & 7);
      out[31 + outPos] = ((in[ 2 + inPos] >>> 29) & 7);
    }
  }

  private static final class Packer4 extends IntPacker {

    private Packer4() {
      super(4);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 15) <<   0)
        | ((in[ 1 + inPos] & 15) <<   4)
        | ((in[ 2 + inPos] & 15) <<   8)
        | ((in[ 3 + inPos] & 15) <<  12)
        | ((in[ 4 + inPos] & 15) <<  16)
        | ((in[ 5 + inPos] & 15) <<  20)
        | ((in[ 6 + inPos] & 15) <<  24)
        | ((in[ 7 + inPos] & 15) <<  28);
      out[ 1 + outPos] =
          ((in[ 8 + inPos] & 15) <<   0)
        | ((in[ 9 + inPos] & 15) <<   4)
        | ((in[10 + inPos] & 15) <<   8)
        | ((in[11 + inPos] & 15) <<  12)
        | ((in[12 + inPos] & 15) <<  16)
        | ((in[13 + inPos] & 15) <<  20)
        | ((in[14 + inPos] & 15) <<  24)
        | ((in[15 + inPos] & 15) <<  28);
      out[ 2 + outPos] =
          ((in[16 + inPos] & 15) <<   0)
        | ((in[17 + inPos] & 15) <<   4)
        | ((in[18 + inPos] & 15) <<   8)
        | ((in[19 + inPos] & 15) <<  12)
        | ((in[20 + inPos] & 15) <<  16)
        | ((in[21 + inPos] & 15) <<  20)
        | ((in[22 + inPos] & 15) <<  24)
        | ((in[23 + inPos] & 15) <<  28);
      out[ 3 + outPos] =
          ((in[24 + inPos] & 15) <<   0)
        | ((in[25 + inPos] & 15) <<   4)
        | ((in[26 + inPos] & 15) <<   8)
        | ((in[27 + inPos] & 15) <<  12)
        | ((in[28 + inPos] & 15) <<  16)
        | ((in[29 + inPos] & 15) <<  20)
        | ((in[30 + inPos] & 15) <<  24)
        | ((in[31 + inPos] & 15) <<  28);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 15);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  4) & 15);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>>  8) & 15);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 12) & 15);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>> 16) & 15);
      out[ 5 + outPos] = ((in[ 0 + inPos] >>> 20) & 15);
      out[ 6 + outPos] = ((in[ 0 + inPos] >>> 24) & 15);
      out[ 7 + outPos] = ((in[ 0 + inPos] >>> 28) & 15);
      out[ 8 + outPos] = ((in[ 1 + inPos] >>>  0) & 15);
      out[ 9 + outPos] = ((in[ 1 + inPos] >>>  4) & 15);
      out[10 + outPos] = ((in[ 1 + inPos] >>>  8) & 15);
      out[11 + outPos] = ((in[ 1 + inPos] >>> 12) & 15);
      out[12 + outPos] = ((in[ 1 + inPos] >>> 16) & 15);
      out[13 + outPos] = ((in[ 1 + inPos] >>> 20) & 15);
      out[14 + outPos] = ((in[ 1 + inPos] >>> 24) & 15);
      out[15 + outPos] = ((in[ 1 + inPos] >>> 28) & 15);
      out[16 + outPos] = ((in[ 2 + inPos] >>>  0) & 15);
      out[17 + outPos] = ((in[ 2 + inPos] >>>  4) & 15);
      out[18 + outPos] = ((in[ 2 + inPos] >>>  8) & 15);
      out[19 + outPos] = ((in[ 2 + inPos] >>> 12) & 15);
      out[20 + outPos] = ((in[ 2 + inPos] >>> 16) & 15);
      out[21 + outPos] = ((in[ 2 + inPos] >>> 20) & 15);
      out[22 + outPos] = ((in[ 2 + inPos] >>> 24) & 15);
      out[23 + outPos] = ((in[ 2 + inPos] >>> 28) & 15);
      out[24 + outPos] = ((in[ 3 + inPos] >>>  0) & 15);
      out[25 + outPos] = ((in[ 3 + inPos] >>>  4) & 15);
      out[26 + outPos] = ((in[ 3 + inPos] >>>  8) & 15);
      out[27 + outPos] = ((in[ 3 + inPos] >>> 12) & 15);
      out[28 + outPos] = ((in[ 3 + inPos] >>> 16) & 15);
      out[29 + outPos] = ((in[ 3 + inPos] >>> 20) & 15);
      out[30 + outPos] = ((in[ 3 + inPos] >>> 24) & 15);
      out[31 + outPos] = ((in[ 3 + inPos] >>> 28) & 15);
    }
  }

  private static final class Packer5 extends IntPacker {

    private Packer5() {
      super(5);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 31) <<   0)
        | ((in[ 1 + inPos] & 31) <<   5)
        | ((in[ 2 + inPos] & 31) <<  10)
        | ((in[ 3 + inPos] & 31) <<  15)
        | ((in[ 4 + inPos] & 31) <<  20)
        | ((in[ 5 + inPos] & 31) <<  25)
        | ((in[ 6 + inPos] & 31) <<  30);
      out[ 1 + outPos] =
          ((in[ 6 + inPos] & 31) >>>  2)
        | ((in[ 7 + inPos] & 31) <<   3)
        | ((in[ 8 + inPos] & 31) <<   8)
        | ((in[ 9 + inPos] & 31) <<  13)
        | ((in[10 + inPos] & 31) <<  18)
        | ((in[11 + inPos] & 31) <<  23)
        | ((in[12 + inPos] & 31) <<  28);
      out[ 2 + outPos] =
          ((in[12 + inPos] & 31) >>>  4)
        | ((in[13 + inPos] & 31) <<   1)
        | ((in[14 + inPos] & 31) <<   6)
        | ((in[15 + inPos] & 31) <<  11)
        | ((in[16 + inPos] & 31) <<  16)
        | ((in[17 + inPos] & 31) <<  21)
        | ((in[18 + inPos] & 31) <<  26)
        | ((in[19 + inPos] & 31) <<  31);
      out[ 3 + outPos] =
          ((in[19 + inPos] & 31) >>>  1)
        | ((in[20 + inPos] & 31) <<   4)
        | ((in[21 + inPos] & 31) <<   9)
        | ((in[22 + inPos] & 31) <<  14)
        | ((in[23 + inPos] & 31) <<  19)
        | ((in[24 + inPos] & 31) <<  24)
        | ((in[25 + inPos] & 31) <<  29);
      out[ 4 + outPos] =
          ((in[25 + inPos] & 31) >>>  3)
        | ((in[26 + inPos] & 31) <<   2)
        | ((in[27 + inPos] & 31) <<   7)
        | ((in[28 + inPos] & 31) <<  12)
        | ((in[29 + inPos] & 31) <<  17)
        | ((in[30 + inPos] & 31) <<  22)
        | ((in[31 + inPos] & 31) <<  27);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 31);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  5) & 31);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 10) & 31);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 15) & 31);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>> 20) & 31);
      out[ 5 + outPos] = ((in[ 0 + inPos] >>> 25) & 31);
      out[ 6 + outPos] = ((in[ 0 + inPos] >>> 30) & 31) | ((in[ 1 + inPos] & 7) <<  2);
      out[ 7 + outPos] = ((in[ 1 + inPos] >>>  3) & 31);
      out[ 8 + outPos] = ((in[ 1 + inPos] >>>  8) & 31);
      out[ 9 + outPos] = ((in[ 1 + inPos] >>> 13) & 31);
      out[10 + outPos] = ((in[ 1 + inPos] >>> 18) & 31);
      out[11 + outPos] = ((in[ 1 + inPos] >>> 23) & 31);
      out[12 + outPos] = ((in[ 1 + inPos] >>> 28) & 31) | ((in[ 2 + inPos] & 1) <<  4);
      out[13 + outPos] = ((in[ 2 + inPos] >>>  1) & 31);
      out[14 + outPos] = ((in[ 2 + inPos] >>>  6) & 31);
      out[15 + outPos] = ((in[ 2 + inPos] >>> 11) & 31);
      out[16 + outPos] = ((in[ 2 + inPos] >>> 16) & 31);
      out[17 + outPos] = ((in[ 2 + inPos] >>> 21) & 31);
      out[18 + outPos] = ((in[ 2 + inPos] >>> 26) & 31);
      out[19 + outPos] = ((in[ 2 + inPos] >>> 31) & 31) | ((in[ 3 + inPos] & 15) <<  1);
      out[20 + outPos] = ((in[ 3 + inPos] >>>  4) & 31);
      out[21 + outPos] = ((in[ 3 + inPos] >>>  9) & 31);
      out[22 + outPos] = ((in[ 3 + inPos] >>> 14) & 31);
      out[23 + outPos] = ((in[ 3 + inPos] >>> 19) & 31);
      out[24 + outPos] = ((in[ 3 + inPos] >>> 24) & 31);
      out[25 + outPos] = ((in[ 3 + inPos] >>> 29) & 31) | ((in[ 4 + inPos] & 3) <<  3);
      out[26 + outPos] = ((in[ 4 + inPos] >>>  2) & 31);
      out[27 + outPos] = ((in[ 4 + inPos] >>>  7) & 31);
      out[28 + outPos] = ((in[ 4 + inPos] >>> 12) & 31);
      out[29 + outPos] = ((in[ 4 + inPos] >>> 17) & 31);
      out[30 + outPos] = ((in[ 4 + inPos] >>> 22) & 31);
      out[31 + outPos] = ((in[ 4 + inPos] >>> 27) & 31);
    }
  }

  private static final class Packer6 extends IntPacker {

    private Packer6() {
      super(6);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 63) <<   0)
        | ((in[ 1 + inPos] & 63) <<   6)
        | ((in[ 2 + inPos] & 63) <<  12)
        | ((in[ 3 + inPos] & 63) <<  18)
        | ((in[ 4 + inPos] & 63) <<  24)
        | ((in[ 5 + inPos] & 63) <<  30);
      out[ 1 + outPos] =
          ((in[ 5 + inPos] & 63) >>>  2)
        | ((in[ 6 + inPos] & 63) <<   4)
        | ((in[ 7 + inPos] & 63) <<  10)
        | ((in[ 8 + inPos] & 63) <<  16)
        | ((in[ 9 + inPos] & 63) <<  22)
        | ((in[10 + inPos] & 63) <<  28);
      out[ 2 + outPos] =
          ((in[10 + inPos] & 63) >>>  4)
        | ((in[11 + inPos] & 63) <<   2)
        | ((in[12 + inPos] & 63) <<   8)
        | ((in[13 + inPos] & 63) <<  14)
        | ((in[14 + inPos] & 63) <<  20)
        | ((in[15 + inPos] & 63) <<  26);
      out[ 3 + outPos] =
          ((in[16 + inPos] & 63) <<   0)
        | ((in[17 + inPos] & 63) <<   6)
        | ((in[18 + inPos] & 63) <<  12)
        | ((in[19 + inPos] & 63) <<  18)
        | ((in[20 + inPos] & 63) <<  24)
        | ((in[21 + inPos] & 63) <<  30);
      out[ 4 + outPos] =
          ((in[21 + inPos] & 63) >>>  2)
        | ((in[22 + inPos] & 63) <<   4)
        | ((in[23 + inPos] & 63) <<  10)
        | ((in[24 + inPos] & 63) <<  16)
        | ((in[25 + inPos] & 63) <<  22)
        | ((in[26 + inPos] & 63) <<  28);
      out[ 5 + outPos] =
          ((in[26 + inPos] & 63) >>>  4)
        | ((in[27 + inPos] & 63) <<   2)
        | ((in[28 + inPos] & 63) <<   8)
        | ((in[29 + inPos] & 63) <<  14)
        | ((in[30 + inPos] & 63) <<  20)
        | ((in[31 + inPos] & 63) <<  26);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 63);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  6) & 63);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 12) & 63);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 18) & 63);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>> 24) & 63);
      out[ 5 + outPos] = ((in[ 0 + inPos] >>> 30) & 63) | ((in[ 1 + inPos] & 15) <<  2);
      out[ 6 + outPos] = ((in[ 1 + inPos] >>>  4) & 63);
      out[ 7 + outPos] = ((in[ 1 + inPos] >>> 10) & 63);
      out[ 8 + outPos] = ((in[ 1 + inPos] >>> 16) & 63);
      out[ 9 + outPos] = ((in[ 1 + inPos] >>> 22) & 63);
      out[10 + outPos] = ((in[ 1 + inPos] >>> 28) & 63) | ((in[ 2 + inPos] & 3) <<  4);
      out[11 + outPos] = ((in[ 2 + inPos] >>>  2) & 63);
      out[12 + outPos] = ((in[ 2 + inPos] >>>  8) & 63);
      out[13 + outPos] = ((in[ 2 + inPos] >>> 14) & 63);
      out[14 + outPos] = ((in[ 2 + inPos] >>> 20) & 63);
      out[15 + outPos] = ((in[ 2 + inPos] >>> 26) & 63);
      out[16 + outPos] = ((in[ 3 + inPos] >>>  0) & 63);
      out[17 + outPos] = ((in[ 3 + inPos] >>>  6) & 63);
      out[18 + outPos] = ((in[ 3 + inPos] >>> 12) & 63);
      out[19 + outPos] = ((in[ 3 + inPos] >>> 18) & 63);
      out[20 + outPos] = ((in[ 3 + inPos] >>> 24) & 63);
      out[21 + outPos] = ((in[ 3 + inPos] >>> 30) & 63) | ((in[ 4 + inPos] & 15) <<  2);
      out[22 + outPos] = ((in[ 4 + inPos] >>>  4) & 63);
      out[23 + outPos] = ((in[ 4 + inPos] >>> 10) & 63);
      out[24 + outPos] = ((in[ 4 + inPos] >>> 16) & 63);
      out[25 + outPos] = ((in[ 4 + inPos] >>> 22) & 63);
      out[26 + outPos] = ((in[ 4 + inPos] >>> 28) & 63) | ((in[ 5 + inPos] & 3) <<  4);
      out[27 + outPos] = ((in[ 5 + inPos] >>>  2) & 63);
      out[28 + outPos] = ((in[ 5 + inPos] >>>  8) & 63);
      out[29 + outPos] = ((in[ 5 + inPos] >>> 14) & 63);
      out[30 + outPos] = ((in[ 5 + inPos] >>> 20) & 63);
      out[31 + outPos] = ((in[ 5 + inPos] >>> 26) & 63);
    }
  }

  private static final class Packer7 extends IntPacker {

    private Packer7() {
      super(7);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 127) <<   0)
        | ((in[ 1 + inPos] & 127) <<   7)
        | ((in[ 2 + inPos] & 127) <<  14)
        | ((in[ 3 + inPos] & 127) <<  21)
        | ((in[ 4 + inPos] & 127) <<  28);
      out[ 1 + outPos] =
          ((in[ 4 + inPos] & 127) >>>  4)
        | ((in[ 5 + inPos] & 127) <<   3)
        | ((in[ 6 + inPos] & 127) <<  10)
        | ((in[ 7 + inPos] & 127) <<  17)
        | ((in[ 8 + inPos] & 127) <<  24)
        | ((in[ 9 + inPos] & 127) <<  31);
      out[ 2 + outPos] =
          ((in[ 9 + inPos] & 127) >>>  1)
        | ((in[10 + inPos] & 127) <<   6)
        | ((in[11 + inPos] & 127) <<  13)
        | ((in[12 + inPos] & 127) <<  20)
        | ((in[13 + inPos] & 127) <<  27);
      out[ 3 + outPos] =
          ((in[13 + inPos] & 127) >>>  5)
        | ((in[14 + inPos] & 127) <<   2)
        | ((in[15 + inPos] & 127) <<   9)
        | ((in[16 + inPos] & 127) <<  16)
        | ((in[17 + inPos] & 127) <<  23)
        | ((in[18 + inPos] & 127) <<  30);
      out[ 4 + outPos] =
          ((in[18 + inPos] & 127) >>>  2)
        | ((in[19 + inPos] & 127) <<   5)
        | ((in[20 + inPos] & 127) <<  12)
        | ((in[21 + inPos] & 127) <<  19)
        | ((in[22 + inPos] & 127) <<  26);
      out[ 5 + outPos] =
          ((in[22 + inPos] & 127) >>>  6)
        | ((in[23 + inPos] & 127) <<   1)
        | ((in[24 + inPos] & 127) <<   8)
        | ((in[25 + inPos] & 127) <<  15)
        | ((in[26 + inPos] & 127) <<  22)
        | ((in[27 + inPos] & 127) <<  29);
      out[ 6 + outPos] =
          ((in[27 + inPos] & 127) >>>  3)
        | ((in[28 + inPos] & 127) <<   4)
        | ((in[29 + inPos] & 127) <<  11)
        | ((in[30 + inPos] & 127) <<  18)
        | ((in[31 + inPos] & 127) <<  25);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 127);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  7) & 127);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 14) & 127);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 21) & 127);
      out[ 4 + outPos] = ((in[ 0 + inPos] >>> 28) & 127) | ((in[ 1 + inPos] & 7) <<  4);
      out[ 5 + outPos] = ((in[ 1 + inPos] >>>  3) & 127);
      out[ 6 + outPos] = ((in[ 1 + inPos] >>> 10) & 127);
      out[ 7 + outPos] = ((in[ 1 + inPos] >>> 17) & 127);
      out[ 8 + outPos] = ((in[ 1 + inPos] >>> 24) & 127);
      out[ 9 + outPos] = ((in[ 1 + inPos] >>> 31) & 127) | ((in[ 2 + inPos] & 63) <<  1);
      out[10 + outPos] = ((in[ 2 + inPos] >>>  6) & 127);
      out[11 + outPos] = ((in[ 2 + inPos] >>> 13) & 127);
      out[12 + outPos] = ((in[ 2 + inPos] >>> 20) & 127);
      out[13 + outPos] = ((in[ 2 + inPos] >>> 27) & 127) | ((in[ 3 + inPos] & 3) <<  5);
      out[14 + outPos] = ((in[ 3 + inPos] >>>  2) & 127);
      out[15 + outPos] = ((in[ 3 + inPos] >>>  9) & 127);
      out[16 + outPos] = ((in[ 3 + inPos] >>> 16) & 127);
      out[17 + outPos] = ((in[ 3 + inPos] >>> 23) & 127);
      out[18 + outPos] = ((in[ 3 + inPos] >>> 30) & 127) | ((in[ 4 + inPos] & 31) <<  2);
      out[19 + outPos] = ((in[ 4 + inPos] >>>  5) & 127);
      out[20 + outPos] = ((in[ 4 + inPos] >>> 12) & 127);
      out[21 + outPos] = ((in[ 4 + inPos] >>> 19) & 127);
      out[22 + outPos] = ((in[ 4 + inPos] >>> 26) & 127) | ((in[ 5 + inPos] & 1) <<  6);
      out[23 + outPos] = ((in[ 5 + inPos] >>>  1) & 127);
      out[24 + outPos] = ((in[ 5 + inPos] >>>  8) & 127);
      out[25 + outPos] = ((in[ 5 + inPos] >>> 15) & 127);
      out[26 + outPos] = ((in[ 5 + inPos] >>> 22) & 127);
      out[27 + outPos] = ((in[ 5 + inPos] >>> 29) & 127) | ((in[ 6 + inPos] & 15) <<  3);
      out[28 + outPos] = ((in[ 6 + inPos] >>>  4) & 127);
      out[29 + outPos] = ((in[ 6 + inPos] >>> 11) & 127);
      out[30 + outPos] = ((in[ 6 + inPos] >>> 18) & 127);
      out[31 + outPos] = ((in[ 6 + inPos] >>> 25) & 127);
    }
  }

  private static final class Packer8 extends IntPacker {

    private Packer8() {
      super(8);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 255) <<   0)
        | ((in[ 1 + inPos] & 255) <<   8)
        | ((in[ 2 + inPos] & 255) <<  16)
        | ((in[ 3 + inPos] & 255) <<  24);
      out[ 1 + outPos] =
          ((in[ 4 + inPos] & 255) <<   0)
        | ((in[ 5 + inPos] & 255) <<   8)
        | ((in[ 6 + inPos] & 255) <<  16)
        | ((in[ 7 + inPos] & 255) <<  24);
      out[ 2 + outPos] =
          ((in[ 8 + inPos] & 255) <<   0)
        | ((in[ 9 + inPos] & 255) <<   8)
        | ((in[10 + inPos] & 255) <<  16)
        | ((in[11 + inPos] & 255) <<  24);
      out[ 3 + outPos] =
          ((in[12 + inPos] & 255) <<   0)
        | ((in[13 + inPos] & 255) <<   8)
        | ((in[14 + inPos] & 255) <<  16)
        | ((in[15 + inPos] & 255) <<  24);
      out[ 4 + outPos] =
          ((in[16 + inPos] & 255) <<   0)
        | ((in[17 + inPos] & 255) <<   8)
        | ((in[18 + inPos] & 255) <<  16)
        | ((in[19 + inPos] & 255) <<  24);
      out[ 5 + outPos] =
          ((in[20 + inPos] & 255) <<   0)
        | ((in[21 + inPos] & 255) <<   8)
        | ((in[22 + inPos] & 255) <<  16)
        | ((in[23 + inPos] & 255) <<  24);
      out[ 6 + outPos] =
          ((in[24 + inPos] & 255) <<   0)
        | ((in[25 + inPos] & 255) <<   8)
        | ((in[26 + inPos] & 255) <<  16)
        | ((in[27 + inPos] & 255) <<  24);
      out[ 7 + outPos] =
          ((in[28 + inPos] & 255) <<   0)
        | ((in[29 + inPos] & 255) <<   8)
        | ((in[30 + inPos] & 255) <<  16)
        | ((in[31 + inPos] & 255) <<  24);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 255);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  8) & 255);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 16) & 255);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 24) & 255);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>>  0) & 255);
      out[ 5 + outPos] = ((in[ 1 + inPos] >>>  8) & 255);
      out[ 6 + outPos] = ((in[ 1 + inPos] >>> 16) & 255);
      out[ 7 + outPos] = ((in[ 1 + inPos] >>> 24) & 255);
      out[ 8 + outPos] = ((in[ 2 + inPos] >>>  0) & 255);
      out[ 9 + outPos] = ((in[ 2 + inPos] >>>  8) & 255);
      out[10 + outPos] = ((in[ 2 + inPos] >>> 16) & 255);
      out[11 + outPos] = ((in[ 2 + inPos] >>> 24) & 255);
      out[12 + outPos] = ((in[ 3 + inPos] >>>  0) & 255);
      out[13 + outPos] = ((in[ 3 + inPos] >>>  8) & 255);
      out[14 + outPos] = ((in[ 3 + inPos] >>> 16) & 255);
      out[15 + outPos] = ((in[ 3 + inPos] >>> 24) & 255);
      out[16 + outPos] = ((in[ 4 + inPos] >>>  0) & 255);
      out[17 + outPos] = ((in[ 4 + inPos] >>>  8) & 255);
      out[18 + outPos] = ((in[ 4 + inPos] >>> 16) & 255);
      out[19 + outPos] = ((in[ 4 + inPos] >>> 24) & 255);
      out[20 + outPos] = ((in[ 5 + inPos] >>>  0) & 255);
      out[21 + outPos] = ((in[ 5 + inPos] >>>  8) & 255);
      out[22 + outPos] = ((in[ 5 + inPos] >>> 16) & 255);
      out[23 + outPos] = ((in[ 5 + inPos] >>> 24) & 255);
      out[24 + outPos] = ((in[ 6 + inPos] >>>  0) & 255);
      out[25 + outPos] = ((in[ 6 + inPos] >>>  8) & 255);
      out[26 + outPos] = ((in[ 6 + inPos] >>> 16) & 255);
      out[27 + outPos] = ((in[ 6 + inPos] >>> 24) & 255);
      out[28 + outPos] = ((in[ 7 + inPos] >>>  0) & 255);
      out[29 + outPos] = ((in[ 7 + inPos] >>>  8) & 255);
      out[30 + outPos] = ((in[ 7 + inPos] >>> 16) & 255);
      out[31 + outPos] = ((in[ 7 + inPos] >>> 24) & 255);
    }
  }

  private static final class Packer9 extends IntPacker {

    private Packer9() {
      super(9);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 511) <<   0)
        | ((in[ 1 + inPos] & 511) <<   9)
        | ((in[ 2 + inPos] & 511) <<  18)
        | ((in[ 3 + inPos] & 511) <<  27);
      out[ 1 + outPos] =
          ((in[ 3 + inPos] & 511) >>>  5)
        | ((in[ 4 + inPos] & 511) <<   4)
        | ((in[ 5 + inPos] & 511) <<  13)
        | ((in[ 6 + inPos] & 511) <<  22)
        | ((in[ 7 + inPos] & 511) <<  31);
      out[ 2 + outPos] =
          ((in[ 7 + inPos] & 511) >>>  1)
        | ((in[ 8 + inPos] & 511) <<   8)
        | ((in[ 9 + inPos] & 511) <<  17)
        | ((in[10 + inPos] & 511) <<  26);
      out[ 3 + outPos] =
          ((in[10 + inPos] & 511) >>>  6)
        | ((in[11 + inPos] & 511) <<   3)
        | ((in[12 + inPos] & 511) <<  12)
        | ((in[13 + inPos] & 511) <<  21)
        | ((in[14 + inPos] & 511) <<  30);
      out[ 4 + outPos] =
          ((in[14 + inPos] & 511) >>>  2)
        | ((in[15 + inPos] & 511) <<   7)
        | ((in[16 + inPos] & 511) <<  16)
        | ((in[17 + inPos] & 511) <<  25);
      out[ 5 + outPos] =
          ((in[17 + inPos] & 511) >>>  7)
        | ((in[18 + inPos] & 511) <<   2)
        | ((in[19 + inPos] & 511) <<  11)
        | ((in[20 + inPos] & 511) <<  20)
        | ((in[21 + inPos] & 511) <<  29);
      out[ 6 + outPos] =
          ((in[21 + inPos] & 511) >>>  3)
        | ((in[22 + inPos] & 511) <<   6)
        | ((in[23 + inPos] & 511) <<  15)
        | ((in[24 + inPos] & 511) <<  24);
      out[ 7 + outPos] =
          ((in[24 + inPos] & 511) >>>  8)
        | ((in[25 + inPos] & 511) <<   1)
        | ((in[26 + inPos] & 511) <<  10)
        | ((in[27 + inPos] & 511) <<  19)
        | ((in[28 + inPos] & 511) <<  28);
      out[ 8 + outPos] =
          ((in[28 + inPos] & 511) >>>  4)
        | ((in[29 + inPos] & 511) <<   5)
        | ((in[30 + inPos] & 511) <<  14)
        | ((in[31 + inPos] & 511) <<  23);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 511);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>>  9) & 511);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 18) & 511);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 27) & 511) | ((in[ 1 + inPos] & 15) <<  5);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>>  4) & 511);
      out[ 5 + outPos] = ((in[ 1 + inPos] >>> 13) & 511);
      out[ 6 + outPos] = ((in[ 1 + inPos] >>> 22) & 511);
      out[ 7 + outPos] = ((in[ 1 + inPos] >>> 31) & 511) | ((in[ 2 + inPos] & 255) <<  1);
      out[ 8 + outPos] = ((in[ 2 + inPos] >>>  8) & 511);
      out[ 9 + outPos] = ((in[ 2 + inPos] >>> 17) & 511);
      out[10 + outPos] = ((in[ 2 + inPos] >>> 26) & 511) | ((in[ 3 + inPos] & 7) <<  6);
      out[11 + outPos] = ((in[ 3 + inPos] >>>  3) & 511);
      out[12 + outPos] = ((in[ 3 + inPos] >>> 12) & 511);
      out[13 + outPos] = ((in[ 3 + inPos] >>> 21) & 511);
      out[14 + outPos] = ((in[ 3 + inPos] >>> 30) & 511) | ((in[ 4 + inPos] & 127) <<  2);
      out[15 + outPos] = ((in[ 4 + inPos] >>>  7) & 511);
      out[16 + outPos] = ((in[ 4 + inPos] >>> 16) & 511);
      out[17 + outPos] = ((in[ 4 + inPos] >>> 25) & 511) | ((in[ 5 + inPos] & 3) <<  7);
      out[18 + outPos] = ((in[ 5 + inPos] >>>  2) & 511);
      out[19 + outPos] = ((in[ 5 + inPos] >>> 11) & 511);
      out[20 + outPos] = ((in[ 5 + inPos] >>> 20) & 511);
      out[21 + outPos] = ((in[ 5 + inPos] >>> 29) & 511) | ((in[ 6 + inPos] & 63) <<  3);
      out[22 + outPos] = ((in[ 6 + inPos] >>>  6) & 511);
      out[23 + outPos] = ((in[ 6 + inPos] >>> 15) & 511);
      out[24 + outPos] = ((in[ 6 + inPos] >>> 24) & 511) | ((in[ 7 + inPos] & 1) <<  8);
      out[25 + outPos] = ((in[ 7 + inPos] >>>  1) & 511);
      out[26 + outPos] = ((in[ 7 + inPos] >>> 10) & 511);
      out[27 + outPos] = ((in[ 7 + inPos] >>> 19) & 511);
      out[28 + outPos] = ((in[ 7 + inPos] >>> 28) & 511) | ((in[ 8 + inPos] & 31) <<  4);
      out[29 + outPos] = ((in[ 8 + inPos] >>>  5) & 511);
      out[30 + outPos] = ((in[ 8 + inPos] >>> 14) & 511);
      out[31 + outPos] = ((in[ 8 + inPos] >>> 23) & 511);
    }
  }

  private static final class Packer10 extends IntPacker {

    private Packer10() {
      super(10);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 1023) <<   0)
        | ((in[ 1 + inPos] & 1023) <<  10)
        | ((in[ 2 + inPos] & 1023) <<  20)
        | ((in[ 3 + inPos] & 1023) <<  30);
      out[ 1 + outPos] =
          ((in[ 3 + inPos] & 1023) >>>  2)
        | ((in[ 4 + inPos] & 1023) <<   8)
        | ((in[ 5 + inPos] & 1023) <<  18)
        | ((in[ 6 + inPos] & 1023) <<  28);
      out[ 2 + outPos] =
          ((in[ 6 + inPos] & 1023) >>>  4)
        | ((in[ 7 + inPos] & 1023) <<   6)
        | ((in[ 8 + inPos] & 1023) <<  16)
        | ((in[ 9 + inPos] & 1023) <<  26);
      out[ 3 + outPos] =
          ((in[ 9 + inPos] & 1023) >>>  6)
        | ((in[10 + inPos] & 1023) <<   4)
        | ((in[11 + inPos] & 1023) <<  14)
        | ((in[12 + inPos] & 1023) <<  24);
      out[ 4 + outPos] =
          ((in[12 + inPos] & 1023) >>>  8)
        | ((in[13 + inPos] & 1023) <<   2)
        | ((in[14 + inPos] & 1023) <<  12)
        | ((in[15 + inPos] & 1023) <<  22);
      out[ 5 + outPos] =
          ((in[16 + inPos] & 1023) <<   0)
        | ((in[17 + inPos] & 1023) <<  10)
        | ((in[18 + inPos] & 1023) <<  20)
        | ((in[19 + inPos] & 1023) <<  30);
      out[ 6 + outPos] =
          ((in[19 + inPos] & 1023) >>>  2)
        | ((in[20 + inPos] & 1023) <<   8)
        | ((in[21 + inPos] & 1023) <<  18)
        | ((in[22 + inPos] & 1023) <<  28);
      out[ 7 + outPos] =
          ((in[22 + inPos] & 1023) >>>  4)
        | ((in[23 + inPos] & 1023) <<   6)
        | ((in[24 + inPos] & 1023) <<  16)
        | ((in[25 + inPos] & 1023) <<  26);
      out[ 8 + outPos] =
          ((in[25 + inPos] & 1023) >>>  6)
        | ((in[26 + inPos] & 1023) <<   4)
        | ((in[27 + inPos] & 1023) <<  14)
        | ((in[28 + inPos] & 1023) <<  24);
      out[ 9 + outPos] =
          ((in[28 + inPos] & 1023) >>>  8)
        | ((in[29 + inPos] & 1023) <<   2)
        | ((in[30 + inPos] & 1023) <<  12)
        | ((in[31 + inPos] & 1023) <<  22);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 1023);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 10) & 1023);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 20) & 1023);
      out[ 3 + outPos] = ((in[ 0 + inPos] >>> 30) & 1023) | ((in[ 1 + inPos] & 255) <<  2);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>>  8) & 1023);
      out[ 5 + outPos] = ((in[ 1 + inPos] >>> 18) & 1023);
      out[ 6 + outPos] = ((in[ 1 + inPos] >>> 28) & 1023) | ((in[ 2 + inPos] & 63) <<  4);
      out[ 7 + outPos] = ((in[ 2 + inPos] >>>  6) & 1023);
      out[ 8 + outPos] = ((in[ 2 + inPos] >>> 16) & 1023);
      out[ 9 + outPos] = ((in[ 2 + inPos] >>> 26) & 1023) | ((in[ 3 + inPos] & 15) <<  6);
      out[10 + outPos] = ((in[ 3 + inPos] >>>  4) & 1023);
      out[11 + outPos] = ((in[ 3 + inPos] >>> 14) & 1023);
      out[12 + outPos] = ((in[ 3 + inPos] >>> 24) & 1023) | ((in[ 4 + inPos] & 3) <<  8);
      out[13 + outPos] = ((in[ 4 + inPos] >>>  2) & 1023);
      out[14 + outPos] = ((in[ 4 + inPos] >>> 12) & 1023);
      out[15 + outPos] = ((in[ 4 + inPos] >>> 22) & 1023);
      out[16 + outPos] = ((in[ 5 + inPos] >>>  0) & 1023);
      out[17 + outPos] = ((in[ 5 + inPos] >>> 10) & 1023);
      out[18 + outPos] = ((in[ 5 + inPos] >>> 20) & 1023);
      out[19 + outPos] = ((in[ 5 + inPos] >>> 30) & 1023) | ((in[ 6 + inPos] & 255) <<  2);
      out[20 + outPos] = ((in[ 6 + inPos] >>>  8) & 1023);
      out[21 + outPos] = ((in[ 6 + inPos] >>> 18) & 1023);
      out[22 + outPos] = ((in[ 6 + inPos] >>> 28) & 1023) | ((in[ 7 + inPos] & 63) <<  4);
      out[23 + outPos] = ((in[ 7 + inPos] >>>  6) & 1023);
      out[24 + outPos] = ((in[ 7 + inPos] >>> 16) & 1023);
      out[25 + outPos] = ((in[ 7 + inPos] >>> 26) & 1023) | ((in[ 8 + inPos] & 15) <<  6);
      out[26 + outPos] = ((in[ 8 + inPos] >>>  4) & 1023);
      out[27 + outPos] = ((in[ 8 + inPos] >>> 14) & 1023);
      out[28 + outPos] = ((in[ 8 + inPos] >>> 24) & 1023) | ((in[ 9 + inPos] & 3) <<  8);
      out[29 + outPos] = ((in[ 9 + inPos] >>>  2) & 1023);
      out[30 + outPos] = ((in[ 9 + inPos] >>> 12) & 1023);
      out[31 + outPos] = ((in[ 9 + inPos] >>> 22) & 1023);
    }
  }

  private static final class Packer11 extends IntPacker {

    private Packer11() {
      super(11);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 2047) <<   0)
        | ((in[ 1 + inPos] & 2047) <<  11)
        | ((in[ 2 + inPos] & 2047) <<  22);
      out[ 1 + outPos] =
          ((in[ 2 + inPos] & 2047) >>> 10)
        | ((in[ 3 + inPos] & 2047) <<   1)
        | ((in[ 4 + inPos] & 2047) <<  12)
        | ((in[ 5 + inPos] & 2047) <<  23);
      out[ 2 + outPos] =
          ((in[ 5 + inPos] & 2047) >>>  9)
        | ((in[ 6 + inPos] & 2047) <<   2)
        | ((in[ 7 + inPos] & 2047) <<  13)
        | ((in[ 8 + inPos] & 2047) <<  24);
      out[ 3 + outPos] =
          ((in[ 8 + inPos] & 2047) >>>  8)
        | ((in[ 9 + inPos] & 2047) <<   3)
        | ((in[10 + inPos] & 2047) <<  14)
        | ((in[11 + inPos] & 2047) <<  25);
      out[ 4 + outPos] =
          ((in[11 + inPos] & 2047) >>>  7)
        | ((in[12 + inPos] & 2047) <<   4)
        | ((in[13 + inPos] & 2047) <<  15)
        | ((in[14 + inPos] & 2047) <<  26);
      out[ 5 + outPos] =
          ((in[14 + inPos] & 2047) >>>  6)
        | ((in[15 + inPos] & 2047) <<   5)
        | ((in[16 + inPos] & 2047) <<  16)
        | ((in[17 + inPos] & 2047) <<  27);
      out[ 6 + outPos] =
          ((in[17 + inPos] & 2047) >>>  5)
        | ((in[18 + inPos] & 2047) <<   6)
        | ((in[19 + inPos] & 2047) <<  17)
        | ((in[20 + inPos] & 2047) <<  28);
      out[ 7 + outPos] =
          ((in[20 + inPos] & 2047) >>>  4)
        | ((in[21 + inPos] & 2047) <<   7)
        | ((in[22 + inPos] & 2047) <<  18)
        | ((in[23 + inPos] & 2047) <<  29);
      out[ 8 + outPos] =
          ((in[23 + inPos] & 2047) >>>  3)
        | ((in[24 + inPos] & 2047) <<   8)
        | ((in[25 + inPos] & 2047) <<  19)
        | ((in[26 + inPos] & 2047) <<  30);
      out[ 9 + outPos] =
          ((in[26 + inPos] & 2047) >>>  2)
        | ((in[27 + inPos] & 2047) <<   9)
        | ((in[28 + inPos] & 2047) <<  20)
        | ((in[29 + inPos] & 2047) <<  31);
      out[10 + outPos] =
          ((in[29 + inPos] & 2047) >>>  1)
        | ((in[30 + inPos] & 2047) <<  10)
        | ((in[31 + inPos] & 2047) <<  21);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 2047);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 11) & 2047);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 22) & 2047) | ((in[ 1 + inPos] & 1) << 10);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>>  1) & 2047);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>> 12) & 2047);
      out[ 5 + outPos] = ((in[ 1 + inPos] >>> 23) & 2047) | ((in[ 2 + inPos] & 3) <<  9);
      out[ 6 + outPos] = ((in[ 2 + inPos] >>>  2) & 2047);
      out[ 7 + outPos] = ((in[ 2 + inPos] >>> 13) & 2047);
      out[ 8 + outPos] = ((in[ 2 + inPos] >>> 24) & 2047) | ((in[ 3 + inPos] & 7) <<  8);
      out[ 9 + outPos] = ((in[ 3 + inPos] >>>  3) & 2047);
      out[10 + outPos] = ((in[ 3 + inPos] >>> 14) & 2047);
      out[11 + outPos] = ((in[ 3 + inPos] >>> 25) & 2047) | ((in[ 4 + inPos] & 15) <<  7);
      out[12 + outPos] = ((in[ 4 + inPos] >>>  4) & 2047);
      out[13 + outPos] = ((in[ 4 + inPos] >>> 15) & 2047);
      out[14 + outPos] = ((in[ 4 + inPos] >>> 26) & 2047) | ((in[ 5 + inPos] & 31) <<  6);
      out[15 + outPos] = ((in[ 5 + inPos] >>>  5) & 2047);
      out[16 + outPos] = ((in[ 5 + inPos] >>> 16) & 2047);
      out[17 + outPos] = ((in[ 5 + inPos] >>> 27) & 2047) | ((in[ 6 + inPos] & 63) <<  5);
      out[18 + outPos] = ((in[ 6 + inPos] >>>  6) & 2047);
      out[19 + outPos] = ((in[ 6 + inPos] >>> 17) & 2047);
      out[20 + outPos] = ((in[ 6 + inPos] >>> 28) & 2047) | ((in[ 7 + inPos] & 127) <<  4);
      out[21 + outPos] = ((in[ 7 + inPos] >>>  7) & 2047);
      out[22 + outPos] = ((in[ 7 + inPos] >>> 18) & 2047);
      out[23 + outPos] = ((in[ 7 + inPos] >>> 29) & 2047) | ((in[ 8 + inPos] & 255) <<  3);
      out[24 + outPos] = ((in[ 8 + inPos] >>>  8) & 2047);
      out[25 + outPos] = ((in[ 8 + inPos] >>> 19) & 2047);
      out[26 + outPos] = ((in[ 8 + inPos] >>> 30) & 2047) | ((in[ 9 + inPos] & 511) <<  2);
      out[27 + outPos] = ((in[ 9 + inPos] >>>  9) & 2047);
      out[28 + outPos] = ((in[ 9 + inPos] >>> 20) & 2047);
      out[29 + outPos] = ((in[ 9 + inPos] >>> 31) & 2047) | ((in[10 + inPos] & 1023) <<  1);
      out[30 + outPos] = ((in[10 + inPos] >>> 10) & 2047);
      out[31 + outPos] = ((in[10 + inPos] >>> 21) & 2047);
    }
  }

  private static final class Packer12 extends IntPacker {

    private Packer12() {
      super(12);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 4095) <<   0)
        | ((in[ 1 + inPos] & 4095) <<  12)
        | ((in[ 2 + inPos] & 4095) <<  24);
      out[ 1 + outPos] =
          ((in[ 2 + inPos] & 4095) >>>  8)
        | ((in[ 3 + inPos] & 4095) <<   4)
        | ((in[ 4 + inPos] & 4095) <<  16)
        | ((in[ 5 + inPos] & 4095) <<  28);
      out[ 2 + outPos] =
          ((in[ 5 + inPos] & 4095) >>>  4)
        | ((in[ 6 + inPos] & 4095) <<   8)
        | ((in[ 7 + inPos] & 4095) <<  20);
      out[ 3 + outPos] =
          ((in[ 8 + inPos] & 4095) <<   0)
        | ((in[ 9 + inPos] & 4095) <<  12)
        | ((in[10 + inPos] & 4095) <<  24);
      out[ 4 + outPos] =
          ((in[10 + inPos] & 4095) >>>  8)
        | ((in[11 + inPos] & 4095) <<   4)
        | ((in[12 + inPos] & 4095) <<  16)
        | ((in[13 + inPos] & 4095) <<  28);
      out[ 5 + outPos] =
          ((in[13 + inPos] & 4095) >>>  4)
        | ((in[14 + inPos] & 4095) <<   8)
        | ((in[15 + inPos] & 4095) <<  20);
      out[ 6 + outPos] =
          ((in[16 + inPos] & 4095) <<   0)
        | ((in[17 + inPos] & 4095) <<  12)
        | ((in[18 + inPos] & 4095) <<  24);
      out[ 7 + outPos] =
          ((in[18 + inPos] & 4095) >>>  8)
        | ((in[19 + inPos] & 4095) <<   4)
        | ((in[20 + inPos] & 4095) <<  16)
        | ((in[21 + inPos] & 4095) <<  28);
      out[ 8 + outPos] =
          ((in[21 + inPos] & 4095) >>>  4)
        | ((in[22 + inPos] & 4095) <<   8)
        | ((in[23 + inPos] & 4095) <<  20);
      out[ 9 + outPos] =
          ((in[24 + inPos] & 4095) <<   0)
        | ((in[25 + inPos] & 4095) <<  12)
        | ((in[26 + inPos] & 4095) <<  24);
      out[10 + outPos] =
          ((in[26 + inPos] & 4095) >>>  8)
        | ((in[27 + inPos] & 4095) <<   4)
        | ((in[28 + inPos] & 4095) <<  16)
        | ((in[29 + inPos] & 4095) <<  28);
      out[11 + outPos] =
          ((in[29 + inPos] & 4095) >>>  4)
        | ((in[30 + inPos] & 4095) <<   8)
        | ((in[31 + inPos] & 4095) <<  20);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 4095);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 12) & 4095);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 24) & 4095) | ((in[ 1 + inPos] & 15) <<  8);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>>  4) & 4095);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>> 16) & 4095);
      out[ 5 + outPos] = ((in[ 1 + inPos] >>> 28) & 4095) | ((in[ 2 + inPos] & 255) <<  4);
      out[ 6 + outPos] = ((in[ 2 + inPos] >>>  8) & 4095);
      out[ 7 + outPos] = ((in[ 2 + inPos] >>> 20) & 4095);
      out[ 8 + outPos] = ((in[ 3 + inPos] >>>  0) & 4095);
      out[ 9 + outPos] = ((in[ 3 + inPos] >>> 12) & 4095);
      out[10 + outPos] = ((in[ 3 + inPos] >>> 24) & 4095) | ((in[ 4 + inPos] & 15) <<  8);
      out[11 + outPos] = ((in[ 4 + inPos] >>>  4) & 4095);
      out[12 + outPos] = ((in[ 4 + inPos] >>> 16) & 4095);
      out[13 + outPos] = ((in[ 4 + inPos] >>> 28) & 4095) | ((in[ 5 + inPos] & 255) <<  4);
      out[14 + outPos] = ((in[ 5 + inPos] >>>  8) & 4095);
      out[15 + outPos] = ((in[ 5 + inPos] >>> 20) & 4095);
      out[16 + outPos] = ((in[ 6 + inPos] >>>  0) & 4095);
      out[17 + outPos] = ((in[ 6 + inPos] >>> 12) & 4095);
      out[18 + outPos] = ((in[ 6 + inPos] >>> 24) & 4095) | ((in[ 7 + inPos] & 15) <<  8);
      out[19 + outPos] = ((in[ 7 + inPos] >>>  4) & 4095);
      out[20 + outPos] = ((in[ 7 + inPos] >>> 16) & 4095);
      out[21 + outPos] = ((in[ 7 + inPos] >>> 28) & 4095) | ((in[ 8 + inPos] & 255) <<  4);
      out[22 + outPos] = ((in[ 8 + inPos] >>>  8) & 4095);
      out[23 + outPos] = ((in[ 8 + inPos] >>> 20) & 4095);
      out[24 + outPos] = ((in[ 9 + inPos] >>>  0) & 4095);
      out[25 + outPos] = ((in[ 9 + inPos] >>> 12) & 4095);
      out[26 + outPos] = ((in[ 9 + inPos] >>> 24) & 4095) | ((in[10 + inPos] & 15) <<  8);
      out[27 + outPos] = ((in[10 + inPos] >>>  4) & 4095);
      out[28 + outPos] = ((in[10 + inPos] >>> 16) & 4095);
      out[29 + outPos] = ((in[10 + inPos] >>> 28) & 4095) | ((in[11 + inPos] & 255) <<  4);
      out[30 + outPos] = ((in[11 + inPos] >>>  8) & 4095);
      out[31 + outPos] = ((in[11 + inPos] >>> 20) & 4095);
    }
  }

  private static final class Packer13 extends IntPacker {

    private Packer13() {
      super(13);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 8191) <<   0)
        | ((in[ 1 + inPos] & 8191) <<  13)
        | ((in[ 2 + inPos] & 8191) <<  26);
      out[ 1 + outPos] =
          ((in[ 2 + inPos] & 8191) >>>  6)
        | ((in[ 3 + inPos] & 8191) <<   7)
        | ((in[ 4 + inPos] & 8191) <<  20);
      out[ 2 + outPos] =
          ((in[ 4 + inPos] & 8191) >>> 12)
        | ((in[ 5 + inPos] & 8191) <<   1)
        | ((in[ 6 + inPos] & 8191) <<  14)
        | ((in[ 7 + inPos] & 8191) <<  27);
      out[ 3 + outPos] =
          ((in[ 7 + inPos] & 8191) >>>  5)
        | ((in[ 8 + inPos] & 8191) <<   8)
        | ((in[ 9 + inPos] & 8191) <<  21);
      out[ 4 + outPos] =
          ((in[ 9 + inPos] & 8191) >>> 11)
        | ((in[10 + inPos] & 8191) <<   2)
        | ((in[11 + inPos] & 8191) <<  15)
        | ((in[12 + inPos] & 8191) <<  28);
      out[ 5 + outPos] =
          ((in[12 + inPos] & 8191) >>>  4)
        | ((in[13 + inPos] & 8191) <<   9)
        | ((in[14 + inPos] & 8191) <<  22);
      out[ 6 + outPos] =
          ((in[14 + inPos] & 8191) >>> 10)
        | ((in[15 + inPos] & 8191) <<   3)
        | ((in[16 + inPos] & 8191) <<  16)
        | ((in[17 + inPos] & 8191) <<  29);
      out[ 7 + outPos] =
          ((in[17 + inPos] & 8191) >>>  3)
        | ((in[18 + inPos] & 8191) <<  10)
        | ((in[19 + inPos] & 8191) <<  23);
      out[ 8 + outPos] =
          ((in[19 + inPos] & 8191) >>>  9)
        | ((in[20 + inPos] & 8191) <<   4)
        | ((in[21 + inPos] & 8191) <<  17)
        | ((in[22 + inPos] & 8191) <<  30);
      out[ 9 + outPos] =
          ((in[22 + inPos] & 8191) >>>  2)
        | ((in[23 + inPos] & 8191) <<  11)
        | ((in[24 + inPos] & 8191) <<  24);
      out[10 + outPos] =
          ((in[24 + inPos] & 8191) >>>  8)
        | ((in[25 + inPos] & 8191) <<   5)
        | ((in[26 + inPos] & 8191) <<  18)
        | ((in[27 + inPos] & 8191) <<  31);
      out[11 + outPos] =
          ((in[27 + inPos] & 8191) >>>  1)
        | ((in[28 + inPos] & 8191) <<  12)
        | ((in[29 + inPos] & 8191) <<  25);
      out[12 + outPos] =
          ((in[29 + inPos] & 8191) >>>  7)
        | ((in[30 + inPos] & 8191) <<   6)
        | ((in[31 + inPos] & 8191) <<  19);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 8191);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 13) & 8191);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 26) & 8191) | ((in[ 1 + inPos] & 127) <<  6);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>>  7) & 8191);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>> 20) & 8191) | ((in[ 2 + inPos] & 1) << 12);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>>  1) & 8191);
      out[ 6 + outPos] = ((in[ 2 + inPos] >>> 14) & 8191);
      out[ 7 + outPos] = ((in[ 2 + inPos] >>> 27) & 8191) | ((in[ 3 + inPos] & 255) <<  5);
      out[ 8 + outPos] = ((in[ 3 + inPos] >>>  8) & 8191);
      out[ 9 + outPos] = ((in[ 3 + inPos] >>> 21) & 8191) | ((in[ 4 + inPos] & 3) << 11);
      out[10 + outPos] = ((in[ 4 + inPos] >>>  2) & 8191);
      out[11 + outPos] = ((in[ 4 + inPos] >>> 15) & 8191);
      out[12 + outPos] = ((in[ 4 + inPos] >>> 28) & 8191) | ((in[ 5 + inPos] & 511) <<  4);
      out[13 + outPos] = ((in[ 5 + inPos] >>>  9) & 8191);
      out[14 + outPos] = ((in[ 5 + inPos] >>> 22) & 8191) | ((in[ 6 + inPos] & 7) << 10);
      out[15 + outPos] = ((in[ 6 + inPos] >>>  3) & 8191);
      out[16 + outPos] = ((in[ 6 + inPos] >>> 16) & 8191);
      out[17 + outPos] = ((in[ 6 + inPos] >>> 29) & 8191) | ((in[ 7 + inPos] & 1023) <<  3);
      out[18 + outPos] = ((in[ 7 + inPos] >>> 10) & 8191);
      out[19 + outPos] = ((in[ 7 + inPos] >>> 23) & 8191) | ((in[ 8 + inPos] & 15) <<  9);
      out[20 + outPos] = ((in[ 8 + inPos] >>>  4) & 8191);
      out[21 + outPos] = ((in[ 8 + inPos] >>> 17) & 8191);
      out[22 + outPos] = ((in[ 8 + inPos] >>> 30) & 8191) | ((in[ 9 + inPos] & 2047) <<  2);
      out[23 + outPos] = ((in[ 9 + inPos] >>> 11) & 8191);
      out[24 + outPos] = ((in[ 9 + inPos] >>> 24) & 8191) | ((in[10 + inPos] & 31) <<  8);
      out[25 + outPos] = ((in[10 + inPos] >>>  5) & 8191);
      out[26 + outPos] = ((in[10 + inPos] >>> 18) & 8191);
      out[27 + outPos] = ((in[10 + inPos] >>> 31) & 8191) | ((in[11 + inPos] & 4095) <<  1);
      out[28 + outPos] = ((in[11 + inPos] >>> 12) & 8191);
      out[29 + outPos] = ((in[11 + inPos] >>> 25) & 8191) | ((in[12 + inPos] & 63) <<  7);
      out[30 + outPos] = ((in[12 + inPos] >>>  6) & 8191);
      out[31 + outPos] = ((in[12 + inPos] >>> 19) & 8191);
    }
  }

  private static final class Packer14 extends IntPacker {

    private Packer14() {
      super(14);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 16383) <<   0)
        | ((in[ 1 + inPos] & 16383) <<  14)
        | ((in[ 2 + inPos] & 16383) <<  28);
      out[ 1 + outPos] =
          ((in[ 2 + inPos] & 16383) >>>  4)
        | ((in[ 3 + inPos] & 16383) <<  10)
        | ((in[ 4 + inPos] & 16383) <<  24);
      out[ 2 + outPos] =
          ((in[ 4 + inPos] & 16383) >>>  8)
        | ((in[ 5 + inPos] & 16383) <<   6)
        | ((in[ 6 + inPos] & 16383) <<  20);
      out[ 3 + outPos] =
          ((in[ 6 + inPos] & 16383) >>> 12)
        | ((in[ 7 + inPos] & 16383) <<   2)
        | ((in[ 8 + inPos] & 16383) <<  16)
        | ((in[ 9 + inPos] & 16383) <<  30);
      out[ 4 + outPos] =
          ((in[ 9 + inPos] & 16383) >>>  2)
        | ((in[10 + inPos] & 16383) <<  12)
        | ((in[11 + inPos] & 16383) <<  26);
      out[ 5 + outPos] =
          ((in[11 + inPos] & 16383) >>>  6)
        | ((in[12 + inPos] & 16383) <<   8)
        | ((in[13 + inPos] & 16383) <<  22);
      out[ 6 + outPos] =
          ((in[13 + inPos] & 16383) >>> 10)
        | ((in[14 + inPos] & 16383) <<   4)
        | ((in[15 + inPos] & 16383) <<  18);
      out[ 7 + outPos] =
          ((in[16 + inPos] & 16383) <<   0)
        | ((in[17 + inPos] & 16383) <<  14)
        | ((in[18 + inPos] & 16383) <<  28);
      out[ 8 + outPos] =
          ((in[18 + inPos] & 16383) >>>  4)
        | ((in[19 + inPos] & 16383) <<  10)
        | ((in[20 + inPos] & 16383) <<  24);
      out[ 9 + outPos] =
          ((in[20 + inPos] & 16383) >>>  8)
        | ((in[21 + inPos] & 16383) <<   6)
        | ((in[22 + inPos] & 16383) <<  20);
      out[10 + outPos] =
          ((in[22 + inPos] & 16383) >>> 12)
        | ((in[23 + inPos] & 16383) <<   2)
        | ((in[24 + inPos] & 16383) <<  16)
        | ((in[25 + inPos] & 16383) <<  30);
      out[11 + outPos] =
          ((in[25 + inPos] & 16383) >>>  2)
        | ((in[26 + inPos] & 16383) <<  12)
        | ((in[27 + inPos] & 16383) <<  26);
      out[12 + outPos] =
          ((in[27 + inPos] & 16383) >>>  6)
        | ((in[28 + inPos] & 16383) <<   8)
        | ((in[29 + inPos] & 16383) <<  22);
      out[13 + outPos] =
          ((in[29 + inPos] & 16383) >>> 10)
        | ((in[30 + inPos] & 16383) <<   4)
        | ((in[31 + inPos] & 16383) <<  18);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 16383);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 14) & 16383);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 28) & 16383) | ((in[ 1 + inPos] & 1023) <<  4);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 10) & 16383);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>> 24) & 16383) | ((in[ 2 + inPos] & 63) <<  8);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>>  6) & 16383);
      out[ 6 + outPos] = ((in[ 2 + inPos] >>> 20) & 16383) | ((in[ 3 + inPos] & 3) << 12);
      out[ 7 + outPos] = ((in[ 3 + inPos] >>>  2) & 16383);
      out[ 8 + outPos] = ((in[ 3 + inPos] >>> 16) & 16383);
      out[ 9 + outPos] = ((in[ 3 + inPos] >>> 30) & 16383) | ((in[ 4 + inPos] & 4095) <<  2);
      out[10 + outPos] = ((in[ 4 + inPos] >>> 12) & 16383);
      out[11 + outPos] = ((in[ 4 + inPos] >>> 26) & 16383) | ((in[ 5 + inPos] & 255) <<  6);
      out[12 + outPos] = ((in[ 5 + inPos] >>>  8) & 16383);
      out[13 + outPos] = ((in[ 5 + inPos] >>> 22) & 16383) | ((in[ 6 + inPos] & 15) << 10);
      out[14 + outPos] = ((in[ 6 + inPos] >>>  4) & 16383);
      out[15 + outPos] = ((in[ 6 + inPos] >>> 18) & 16383);
      out[16 + outPos] = ((in[ 7 + inPos] >>>  0) & 16383);
      out[17 + outPos] = ((in[ 7 + inPos] >>> 14) & 16383);
      out[18 + outPos] = ((in[ 7 + inPos] >>> 28) & 16383) | ((in[ 8 + inPos] & 1023) <<  4);
      out[19 + outPos] = ((in[ 8 + inPos] >>> 10) & 16383);
      out[20 + outPos] = ((in[ 8 + inPos] >>> 24) & 16383) | ((in[ 9 + inPos] & 63) <<  8);
      out[21 + outPos] = ((in[ 9 + inPos] >>>  6) & 16383);
      out[22 + outPos] = ((in[ 9 + inPos] >>> 20) & 16383) | ((in[10 + inPos] & 3) << 12);
      out[23 + outPos] = ((in[10 + inPos] >>>  2) & 16383);
      out[24 + outPos] = ((in[10 + inPos] >>> 16) & 16383);
      out[25 + outPos] = ((in[10 + inPos] >>> 30) & 16383) | ((in[11 + inPos] & 4095) <<  2);
      out[26 + outPos] = ((in[11 + inPos] >>> 12) & 16383);
      out[27 + outPos] = ((in[11 + inPos] >>> 26) & 16383) | ((in[12 + inPos] & 255) <<  6);
      out[28 + outPos] = ((in[12 + inPos] >>>  8) & 16383);
      out[29 + outPos] = ((in[12 + inPos] >>> 22) & 16383) | ((in[13 + inPos] & 15) << 10);
      out[30 + outPos] = ((in[13 + inPos] >>>  4) & 16383);
      out[31 + outPos] = ((in[13 + inPos] >>> 18) & 16383);
    }
  }

  private static final class Packer15 extends IntPacker {

    private Packer15() {
      super(15);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 32767) <<   0)
        | ((in[ 1 + inPos] & 32767) <<  15)
        | ((in[ 2 + inPos] & 32767) <<  30);
      out[ 1 + outPos] =
          ((in[ 2 + inPos] & 32767) >>>  2)
        | ((in[ 3 + inPos] & 32767) <<  13)
        | ((in[ 4 + inPos] & 32767) <<  28);
      out[ 2 + outPos] =
          ((in[ 4 + inPos] & 32767) >>>  4)
        | ((in[ 5 + inPos] & 32767) <<  11)
        | ((in[ 6 + inPos] & 32767) <<  26);
      out[ 3 + outPos] =
          ((in[ 6 + inPos] & 32767) >>>  6)
        | ((in[ 7 + inPos] & 32767) <<   9)
        | ((in[ 8 + inPos] & 32767) <<  24);
      out[ 4 + outPos] =
          ((in[ 8 + inPos] & 32767) >>>  8)
        | ((in[ 9 + inPos] & 32767) <<   7)
        | ((in[10 + inPos] & 32767) <<  22);
      out[ 5 + outPos] =
          ((in[10 + inPos] & 32767) >>> 10)
        | ((in[11 + inPos] & 32767) <<   5)
        | ((in[12 + inPos] & 32767) <<  20);
      out[ 6 + outPos] =
          ((in[12 + inPos] & 32767) >>> 12)
        | ((in[13 + inPos] & 32767) <<   3)
        | ((in[14 + inPos] & 32767) <<  18);
      out[ 7 + outPos] =
          ((in[14 + inPos] & 32767) >>> 14)
        | ((in[15 + inPos] & 32767) <<   1)
        | ((in[16 + inPos] & 32767) <<  16)
        | ((in[17 + inPos] & 32767) <<  31);
      out[ 8 + outPos] =
          ((in[17 + inPos] & 32767) >>>  1)
        | ((in[18 + inPos] & 32767) <<  14)
        | ((in[19 + inPos] & 32767) <<  29);
      out[ 9 + outPos] =
          ((in[19 + inPos] & 32767) >>>  3)
        | ((in[20 + inPos] & 32767) <<  12)
        | ((in[21 + inPos] & 32767) <<  27);
      out[10 + outPos] =
          ((in[21 + inPos] & 32767) >>>  5)
        | ((in[22 + inPos] & 32767) <<  10)
        | ((in[23 + inPos] & 32767) <<  25);
      out[11 + outPos] =
          ((in[23 + inPos] & 32767) >>>  7)
        | ((in[24 + inPos] & 32767) <<   8)
        | ((in[25 + inPos] & 32767) <<  23);
      out[12 + outPos] =
          ((in[25 + inPos] & 32767) >>>  9)
        | ((in[26 + inPos] & 32767) <<   6)
        | ((in[27 + inPos] & 32767) <<  21);
      out[13 + outPos] =
          ((in[27 + inPos] & 32767) >>> 11)
        | ((in[28 + inPos] & 32767) <<   4)
        | ((in[29 + inPos] & 32767) <<  19);
      out[14 + outPos] =
          ((in[29 + inPos] & 32767) >>> 13)
        | ((in[30 + inPos] & 32767) <<   2)
        | ((in[31 + inPos] & 32767) <<  17);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 32767);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 15) & 32767);
      out[ 2 + outPos] = ((in[ 0 + inPos] >>> 30) & 32767) | ((in[ 1 + inPos] & 8191) <<  2);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 13) & 32767);
      out[ 4 + outPos] = ((in[ 1 + inPos] >>> 28) & 32767) | ((in[ 2 + inPos] & 2047) <<  4);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>> 11) & 32767);
      out[ 6 + outPos] = ((in[ 2 + inPos] >>> 26) & 32767) | ((in[ 3 + inPos] & 511) <<  6);
      out[ 7 + outPos] = ((in[ 3 + inPos] >>>  9) & 32767);
      out[ 8 + outPos] = ((in[ 3 + inPos] >>> 24) & 32767) | ((in[ 4 + inPos] & 127) <<  8);
      out[ 9 + outPos] = ((in[ 4 + inPos] >>>  7) & 32767);
      out[10 + outPos] = ((in[ 4 + inPos] >>> 22) & 32767) | ((in[ 5 + inPos] & 31) << 10);
      out[11 + outPos] = ((in[ 5 + inPos] >>>  5) & 32767);
      out[12 + outPos] = ((in[ 5 + inPos] >>> 20) & 32767) | ((in[ 6 + inPos] & 7) << 12);
      out[13 + outPos] = ((in[ 6 + inPos] >>>  3) & 32767);
      out[14 + outPos] = ((in[ 6 + inPos] >>> 18) & 32767) | ((in[ 7 + inPos] & 1) << 14);
      out[15 + outPos] = ((in[ 7 + inPos] >>>  1) & 32767);
      out[16 + outPos] = ((in[ 7 + inPos] >>> 16) & 32767);
      out[17 + outPos] = ((in[ 7 + inPos] >>> 31) & 32767) | ((in[ 8 + inPos] & 16383) <<  1);
      out[18 + outPos] = ((in[ 8 + inPos] >>> 14) & 32767);
      out[19 + outPos] = ((in[ 8 + inPos] >>> 29) & 32767) | ((in[ 9 + inPos] & 4095) <<  3);
      out[20 + outPos] = ((in[ 9 + inPos] >>> 12) & 32767);
      out[21 + outPos] = ((in[ 9 + inPos] >>> 27) & 32767) | ((in[10 + inPos] & 1023) <<  5);
      out[22 + outPos] = ((in[10 + inPos] >>> 10) & 32767);
      out[23 + outPos] = ((in[10 + inPos] >>> 25) & 32767) | ((in[11 + inPos] & 255) <<  7);
      out[24 + outPos] = ((in[11 + inPos] >>>  8) & 32767);
      out[25 + outPos] = ((in[11 + inPos] >>> 23) & 32767) | ((in[12 + inPos] & 63) <<  9);
      out[26 + outPos] = ((in[12 + inPos] >>>  6) & 32767);
      out[27 + outPos] = ((in[12 + inPos] >>> 21) & 32767) | ((in[13 + inPos] & 15) << 11);
      out[28 + outPos] = ((in[13 + inPos] >>>  4) & 32767);
      out[29 + outPos] = ((in[13 + inPos] >>> 19) & 32767) | ((in[14 + inPos] & 3) << 13);
      out[30 + outPos] = ((in[14 + inPos] >>>  2) & 32767);
      out[31 + outPos] = ((in[14 + inPos] >>> 17) & 32767);
    }
  }

  private static final class Packer16 extends IntPacker {

    private Packer16() {
      super(16);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 65535) <<   0)
        | ((in[ 1 + inPos] & 65535) <<  16);
      out[ 1 + outPos] =
          ((in[ 2 + inPos] & 65535) <<   0)
        | ((in[ 3 + inPos] & 65535) <<  16);
      out[ 2 + outPos] =
          ((in[ 4 + inPos] & 65535) <<   0)
        | ((in[ 5 + inPos] & 65535) <<  16);
      out[ 3 + outPos] =
          ((in[ 6 + inPos] & 65535) <<   0)
        | ((in[ 7 + inPos] & 65535) <<  16);
      out[ 4 + outPos] =
          ((in[ 8 + inPos] & 65535) <<   0)
        | ((in[ 9 + inPos] & 65535) <<  16);
      out[ 5 + outPos] =
          ((in[10 + inPos] & 65535) <<   0)
        | ((in[11 + inPos] & 65535) <<  16);
      out[ 6 + outPos] =
          ((in[12 + inPos] & 65535) <<   0)
        | ((in[13 + inPos] & 65535) <<  16);
      out[ 7 + outPos] =
          ((in[14 + inPos] & 65535) <<   0)
        | ((in[15 + inPos] & 65535) <<  16);
      out[ 8 + outPos] =
          ((in[16 + inPos] & 65535) <<   0)
        | ((in[17 + inPos] & 65535) <<  16);
      out[ 9 + outPos] =
          ((in[18 + inPos] & 65535) <<   0)
        | ((in[19 + inPos] & 65535) <<  16);
      out[10 + outPos] =
          ((in[20 + inPos] & 65535) <<   0)
        | ((in[21 + inPos] & 65535) <<  16);
      out[11 + outPos] =
          ((in[22 + inPos] & 65535) <<   0)
        | ((in[23 + inPos] & 65535) <<  16);
      out[12 + outPos] =
          ((in[24 + inPos] & 65535) <<   0)
        | ((in[25 + inPos] & 65535) <<  16);
      out[13 + outPos] =
          ((in[26 + inPos] & 65535) <<   0)
        | ((in[27 + inPos] & 65535) <<  16);
      out[14 + outPos] =
          ((in[28 + inPos] & 65535) <<   0)
        | ((in[29 + inPos] & 65535) <<  16);
      out[15 + outPos] =
          ((in[30 + inPos] & 65535) <<   0)
        | ((in[31 + inPos] & 65535) <<  16);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 65535);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 16) & 65535);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>>  0) & 65535);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 16) & 65535);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>>  0) & 65535);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>> 16) & 65535);
      out[ 6 + outPos] = ((in[ 3 + inPos] >>>  0) & 65535);
      out[ 7 + outPos] = ((in[ 3 + inPos] >>> 16) & 65535);
      out[ 8 + outPos] = ((in[ 4 + inPos] >>>  0) & 65535);
      out[ 9 + outPos] = ((in[ 4 + inPos] >>> 16) & 65535);
      out[10 + outPos] = ((in[ 5 + inPos] >>>  0) & 65535);
      out[11 + outPos] = ((in[ 5 + inPos] >>> 16) & 65535);
      out[12 + outPos] = ((in[ 6 + inPos] >>>  0) & 65535);
      out[13 + outPos] = ((in[ 6 + inPos] >>> 16) & 65535);
      out[14 + outPos] = ((in[ 7 + inPos] >>>  0) & 65535);
      out[15 + outPos] = ((in[ 7 + inPos] >>> 16) & 65535);
      out[16 + outPos] = ((in[ 8 + inPos] >>>  0) & 65535);
      out[17 + outPos] = ((in[ 8 + inPos] >>> 16) & 65535);
      out[18 + outPos] = ((in[ 9 + inPos] >>>  0) & 65535);
      out[19 + outPos] = ((in[ 9 + inPos] >>> 16) & 65535);
      out[20 + outPos] = ((in[10 + inPos] >>>  0) & 65535);
      out[21 + outPos] = ((in[10 + inPos] >>> 16) & 65535);
      out[22 + outPos] = ((in[11 + inPos] >>>  0) & 65535);
      out[23 + outPos] = ((in[11 + inPos] >>> 16) & 65535);
      out[24 + outPos] = ((in[12 + inPos] >>>  0) & 65535);
      out[25 + outPos] = ((in[12 + inPos] >>> 16) & 65535);
      out[26 + outPos] = ((in[13 + inPos] >>>  0) & 65535);
      out[27 + outPos] = ((in[13 + inPos] >>> 16) & 65535);
      out[28 + outPos] = ((in[14 + inPos] >>>  0) & 65535);
      out[29 + outPos] = ((in[14 + inPos] >>> 16) & 65535);
      out[30 + outPos] = ((in[15 + inPos] >>>  0) & 65535);
      out[31 + outPos] = ((in[15 + inPos] >>> 16) & 65535);
    }
  }

  private static final class Packer17 extends IntPacker {

    private Packer17() {
      super(17);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 131071) <<   0)
        | ((in[ 1 + inPos] & 131071) <<  17);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 131071) >>> 15)
        | ((in[ 2 + inPos] & 131071) <<   2)
        | ((in[ 3 + inPos] & 131071) <<  19);
      out[ 2 + outPos] =
          ((in[ 3 + inPos] & 131071) >>> 13)
        | ((in[ 4 + inPos] & 131071) <<   4)
        | ((in[ 5 + inPos] & 131071) <<  21);
      out[ 3 + outPos] =
          ((in[ 5 + inPos] & 131071) >>> 11)
        | ((in[ 6 + inPos] & 131071) <<   6)
        | ((in[ 7 + inPos] & 131071) <<  23);
      out[ 4 + outPos] =
          ((in[ 7 + inPos] & 131071) >>>  9)
        | ((in[ 8 + inPos] & 131071) <<   8)
        | ((in[ 9 + inPos] & 131071) <<  25);
      out[ 5 + outPos] =
          ((in[ 9 + inPos] & 131071) >>>  7)
        | ((in[10 + inPos] & 131071) <<  10)
        | ((in[11 + inPos] & 131071) <<  27);
      out[ 6 + outPos] =
          ((in[11 + inPos] & 131071) >>>  5)
        | ((in[12 + inPos] & 131071) <<  12)
        | ((in[13 + inPos] & 131071) <<  29);
      out[ 7 + outPos] =
          ((in[13 + inPos] & 131071) >>>  3)
        | ((in[14 + inPos] & 131071) <<  14)
        | ((in[15 + inPos] & 131071) <<  31);
      out[ 8 + outPos] =
          ((in[15 + inPos] & 131071) >>>  1)
        | ((in[16 + inPos] & 131071) <<  16);
      out[ 9 + outPos] =
          ((in[16 + inPos] & 131071) >>> 16)
        | ((in[17 + inPos] & 131071) <<   1)
        | ((in[18 + inPos] & 131071) <<  18);
      out[10 + outPos] =
          ((in[18 + inPos] & 131071) >>> 14)
        | ((in[19 + inPos] & 131071) <<   3)
        | ((in[20 + inPos] & 131071) <<  20);
      out[11 + outPos] =
          ((in[20 + inPos] & 131071) >>> 12)
        | ((in[21 + inPos] & 131071) <<   5)
        | ((in[22 + inPos] & 131071) <<  22);
      out[12 + outPos] =
          ((in[22 + inPos] & 131071) >>> 10)
        | ((in[23 + inPos] & 131071) <<   7)
        | ((in[24 + inPos] & 131071) <<  24);
      out[13 + outPos] =
          ((in[24 + inPos] & 131071) >>>  8)
        | ((in[25 + inPos] & 131071) <<   9)
        | ((in[26 + inPos] & 131071) <<  26);
      out[14 + outPos] =
          ((in[26 + inPos] & 131071) >>>  6)
        | ((in[27 + inPos] & 131071) <<  11)
        | ((in[28 + inPos] & 131071) <<  28);
      out[15 + outPos] =
          ((in[28 + inPos] & 131071) >>>  4)
        | ((in[29 + inPos] & 131071) <<  13)
        | ((in[30 + inPos] & 131071) <<  30);
      out[16 + outPos] =
          ((in[30 + inPos] & 131071) >>>  2)
        | ((in[31 + inPos] & 131071) <<  15);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 131071);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 17) & 131071) | ((in[ 1 + inPos] & 3) << 15);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>>  2) & 131071);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 19) & 131071) | ((in[ 2 + inPos] & 15) << 13);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>>  4) & 131071);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>> 21) & 131071) | ((in[ 3 + inPos] & 63) << 11);
      out[ 6 + outPos] = ((in[ 3 + inPos] >>>  6) & 131071);
      out[ 7 + outPos] = ((in[ 3 + inPos] >>> 23) & 131071) | ((in[ 4 + inPos] & 255) <<  9);
      out[ 8 + outPos] = ((in[ 4 + inPos] >>>  8) & 131071);
      out[ 9 + outPos] = ((in[ 4 + inPos] >>> 25) & 131071) | ((in[ 5 + inPos] & 1023) <<  7);
      out[10 + outPos] = ((in[ 5 + inPos] >>> 10) & 131071);
      out[11 + outPos] = ((in[ 5 + inPos] >>> 27) & 131071) | ((in[ 6 + inPos] & 4095) <<  5);
      out[12 + outPos] = ((in[ 6 + inPos] >>> 12) & 131071);
      out[13 + outPos] = ((in[ 6 + inPos] >>> 29) & 131071) | ((in[ 7 + inPos] & 16383) <<  3);
      out[14 + outPos] = ((in[ 7 + inPos] >>> 14) & 131071);
      out[15 + outPos] = ((in[ 7 + inPos] >>> 31) & 131071) | ((in[ 8 + inPos] & 65535) <<  1);
      out[16 + outPos] = ((in[ 8 + inPos] >>> 16) & 131071) | ((in[ 9 + inPos] & 1) << 16);
      out[17 + outPos] = ((in[ 9 + inPos] >>>  1) & 131071);
      out[18 + outPos] = ((in[ 9 + inPos] >>> 18) & 131071) | ((in[10 + inPos] & 7) << 14);
      out[19 + outPos] = ((in[10 + inPos] >>>  3) & 131071);
      out[20 + outPos] = ((in[10 + inPos] >>> 20) & 131071) | ((in[11 + inPos] & 31) << 12);
      out[21 + outPos] = ((in[11 + inPos] >>>  5) & 131071);
      out[22 + outPos] = ((in[11 + inPos] >>> 22) & 131071) | ((in[12 + inPos] & 127) << 10);
      out[23 + outPos] = ((in[12 + inPos] >>>  7) & 131071);
      out[24 + outPos] = ((in[12 + inPos] >>> 24) & 131071) | ((in[13 + inPos] & 511) <<  8);
      out[25 + outPos] = ((in[13 + inPos] >>>  9) & 131071);
      out[26 + outPos] = ((in[13 + inPos] >>> 26) & 131071) | ((in[14 + inPos] & 2047) <<  6);
      out[27 + outPos] = ((in[14 + inPos] >>> 11) & 131071);
      out[28 + outPos] = ((in[14 + inPos] >>> 28) & 131071) | ((in[15 + inPos] & 8191) <<  4);
      out[29 + outPos] = ((in[15 + inPos] >>> 13) & 131071);
      out[30 + outPos] = ((in[15 + inPos] >>> 30) & 131071) | ((in[16 + inPos] & 32767) <<  2);
      out[31 + outPos] = ((in[16 + inPos] >>> 15) & 131071);
    }
  }

  private static final class Packer18 extends IntPacker {

    private Packer18() {
      super(18);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 262143) <<   0)
        | ((in[ 1 + inPos] & 262143) <<  18);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 262143) >>> 14)
        | ((in[ 2 + inPos] & 262143) <<   4)
        | ((in[ 3 + inPos] & 262143) <<  22);
      out[ 2 + outPos] =
          ((in[ 3 + inPos] & 262143) >>> 10)
        | ((in[ 4 + inPos] & 262143) <<   8)
        | ((in[ 5 + inPos] & 262143) <<  26);
      out[ 3 + outPos] =
          ((in[ 5 + inPos] & 262143) >>>  6)
        | ((in[ 6 + inPos] & 262143) <<  12)
        | ((in[ 7 + inPos] & 262143) <<  30);
      out[ 4 + outPos] =
          ((in[ 7 + inPos] & 262143) >>>  2)
        | ((in[ 8 + inPos] & 262143) <<  16);
      out[ 5 + outPos] =
          ((in[ 8 + inPos] & 262143) >>> 16)
        | ((in[ 9 + inPos] & 262143) <<   2)
        | ((in[10 + inPos] & 262143) <<  20);
      out[ 6 + outPos] =
          ((in[10 + inPos] & 262143) >>> 12)
        | ((in[11 + inPos] & 262143) <<   6)
        | ((in[12 + inPos] & 262143) <<  24);
      out[ 7 + outPos] =
          ((in[12 + inPos] & 262143) >>>  8)
        | ((in[13 + inPos] & 262143) <<  10)
        | ((in[14 + inPos] & 262143) <<  28);
      out[ 8 + outPos] =
          ((in[14 + inPos] & 262143) >>>  4)
        | ((in[15 + inPos] & 262143) <<  14);
      out[ 9 + outPos] =
          ((in[16 + inPos] & 262143) <<   0)
        | ((in[17 + inPos] & 262143) <<  18);
      out[10 + outPos] =
          ((in[17 + inPos] & 262143) >>> 14)
        | ((in[18 + inPos] & 262143) <<   4)
        | ((in[19 + inPos] & 262143) <<  22);
      out[11 + outPos] =
          ((in[19 + inPos] & 262143) >>> 10)
        | ((in[20 + inPos] & 262143) <<   8)
        | ((in[21 + inPos] & 262143) <<  26);
      out[12 + outPos] =
          ((in[21 + inPos] & 262143) >>>  6)
        | ((in[22 + inPos] & 262143) <<  12)
        | ((in[23 + inPos] & 262143) <<  30);
      out[13 + outPos] =
          ((in[23 + inPos] & 262143) >>>  2)
        | ((in[24 + inPos] & 262143) <<  16);
      out[14 + outPos] =
          ((in[24 + inPos] & 262143) >>> 16)
        | ((in[25 + inPos] & 262143) <<   2)
        | ((in[26 + inPos] & 262143) <<  20);
      out[15 + outPos] =
          ((in[26 + inPos] & 262143) >>> 12)
        | ((in[27 + inPos] & 262143) <<   6)
        | ((in[28 + inPos] & 262143) <<  24);
      out[16 + outPos] =
          ((in[28 + inPos] & 262143) >>>  8)
        | ((in[29 + inPos] & 262143) <<  10)
        | ((in[30 + inPos] & 262143) <<  28);
      out[17 + outPos] =
          ((in[30 + inPos] & 262143) >>>  4)
        | ((in[31 + inPos] & 262143) <<  14);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 262143);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 18) & 262143) | ((in[ 1 + inPos] & 15) << 14);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>>  4) & 262143);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 22) & 262143) | ((in[ 2 + inPos] & 255) << 10);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>>  8) & 262143);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>> 26) & 262143) | ((in[ 3 + inPos] & 4095) <<  6);
      out[ 6 + outPos] = ((in[ 3 + inPos] >>> 12) & 262143);
      out[ 7 + outPos] = ((in[ 3 + inPos] >>> 30) & 262143) | ((in[ 4 + inPos] & 65535) <<  2);
      out[ 8 + outPos] = ((in[ 4 + inPos] >>> 16) & 262143) | ((in[ 5 + inPos] & 3) << 16);
      out[ 9 + outPos] = ((in[ 5 + inPos] >>>  2) & 262143);
      out[10 + outPos] = ((in[ 5 + inPos] >>> 20) & 262143) | ((in[ 6 + inPos] & 63) << 12);
      out[11 + outPos] = ((in[ 6 + inPos] >>>  6) & 262143);
      out[12 + outPos] = ((in[ 6 + inPos] >>> 24) & 262143) | ((in[ 7 + inPos] & 1023) <<  8);
      out[13 + outPos] = ((in[ 7 + inPos] >>> 10) & 262143);
      out[14 + outPos] = ((in[ 7 + inPos] >>> 28) & 262143) | ((in[ 8 + inPos] & 16383) <<  4);
      out[15 + outPos] = ((in[ 8 + inPos] >>> 14) & 262143);
      out[16 + outPos] = ((in[ 9 + inPos] >>>  0) & 262143);
      out[17 + outPos] = ((in[ 9 + inPos] >>> 18) & 262143) | ((in[10 + inPos] & 15) << 14);
      out[18 + outPos] = ((in[10 + inPos] >>>  4) & 262143);
      out[19 + outPos] = ((in[10 + inPos] >>> 22) & 262143) | ((in[11 + inPos] & 255) << 10);
      out[20 + outPos] = ((in[11 + inPos] >>>  8) & 262143);
      out[21 + outPos] = ((in[11 + inPos] >>> 26) & 262143) | ((in[12 + inPos] & 4095) <<  6);
      out[22 + outPos] = ((in[12 + inPos] >>> 12) & 262143);
      out[23 + outPos] = ((in[12 + inPos] >>> 30) & 262143) | ((in[13 + inPos] & 65535) <<  2);
      out[24 + outPos] = ((in[13 + inPos] >>> 16) & 262143) | ((in[14 + inPos] & 3) << 16);
      out[25 + outPos] = ((in[14 + inPos] >>>  2) & 262143);
      out[26 + outPos] = ((in[14 + inPos] >>> 20) & 262143) | ((in[15 + inPos] & 63) << 12);
      out[27 + outPos] = ((in[15 + inPos] >>>  6) & 262143);
      out[28 + outPos] = ((in[15 + inPos] >>> 24) & 262143) | ((in[16 + inPos] & 1023) <<  8);
      out[29 + outPos] = ((in[16 + inPos] >>> 10) & 262143);
      out[30 + outPos] = ((in[16 + inPos] >>> 28) & 262143) | ((in[17 + inPos] & 16383) <<  4);
      out[31 + outPos] = ((in[17 + inPos] >>> 14) & 262143);
    }
  }

  private static final class Packer19 extends IntPacker {

    private Packer19() {
      super(19);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 524287) <<   0)
        | ((in[ 1 + inPos] & 524287) <<  19);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 524287) >>> 13)
        | ((in[ 2 + inPos] & 524287) <<   6)
        | ((in[ 3 + inPos] & 524287) <<  25);
      out[ 2 + outPos] =
          ((in[ 3 + inPos] & 524287) >>>  7)
        | ((in[ 4 + inPos] & 524287) <<  12)
        | ((in[ 5 + inPos] & 524287) <<  31);
      out[ 3 + outPos] =
          ((in[ 5 + inPos] & 524287) >>>  1)
        | ((in[ 6 + inPos] & 524287) <<  18);
      out[ 4 + outPos] =
          ((in[ 6 + inPos] & 524287) >>> 14)
        | ((in[ 7 + inPos] & 524287) <<   5)
        | ((in[ 8 + inPos] & 524287) <<  24);
      out[ 5 + outPos] =
          ((in[ 8 + inPos] & 524287) >>>  8)
        | ((in[ 9 + inPos] & 524287) <<  11)
        | ((in[10 + inPos] & 524287) <<  30);
      out[ 6 + outPos] =
          ((in[10 + inPos] & 524287) >>>  2)
        | ((in[11 + inPos] & 524287) <<  17);
      out[ 7 + outPos] =
          ((in[11 + inPos] & 524287) >>> 15)
        | ((in[12 + inPos] & 524287) <<   4)
        | ((in[13 + inPos] & 524287) <<  23);
      out[ 8 + outPos] =
          ((in[13 + inPos] & 524287) >>>  9)
        | ((in[14 + inPos] & 524287) <<  10)
        | ((in[15 + inPos] & 524287) <<  29);
      out[ 9 + outPos] =
          ((in[15 + inPos] & 524287) >>>  3)
        | ((in[16 + inPos] & 524287) <<  16);
      out[10 + outPos] =
          ((in[16 + inPos] & 524287) >>> 16)
        | ((in[17 + inPos] & 524287) <<   3)
        | ((in[18 + inPos] & 524287) <<  22);
      out[11 + outPos] =
          ((in[18 + inPos] & 524287) >>> 10)
        | ((in[19 + inPos] & 524287) <<   9)
        | ((in[20 + inPos] & 524287) <<  28);
      out[12 + outPos] =
          ((in[20 + inPos] & 524287) >>>  4)
        | ((in[21 + inPos] & 524287) <<  15);
      out[13 + outPos] =
          ((in[21 + inPos] & 524287) >>> 17)
        | ((in[22 + inPos] & 524287) <<   2)
        | ((in[23 + inPos] & 524287) <<  21);
      out[14 + outPos] =
          ((in[23 + inPos] & 524287) >>> 11)
        | ((in[24 + inPos] & 524287) <<   8)
        | ((in[25 + inPos] & 524287) <<  27);
      out[15 + outPos] =
          ((in[25 + inPos] & 524287) >>>  5)
        | ((in[26 + inPos] & 524287) <<  14);
      out[16 + outPos] =
          ((in[26 + inPos] & 524287) >>> 18)
        | ((in[27 + inPos] & 524287) <<   1)
        | ((in[28 + inPos] & 524287) <<  20);
      out[17 + outPos] =
          ((in[28 + inPos] & 524287) >>> 12)
        | ((in[29 + inPos] & 524287) <<   7)
        | ((in[30 + inPos] & 524287) <<  26);
      out[18 + outPos] =
          ((in[30 + inPos] & 524287) >>>  6)
        | ((in[31 + inPos] & 524287) <<  13);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 524287);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 19) & 524287) | ((in[ 1 + inPos] & 63) << 13);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>>  6) & 524287);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 25) & 524287) | ((in[ 2 + inPos] & 4095) <<  7);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>> 12) & 524287);
      out[ 5 + outPos] = ((in[ 2 + inPos] >>> 31) & 524287) | ((in[ 3 + inPos] & 262143) <<  1);
      out[ 6 + outPos] = ((in[ 3 + inPos] >>> 18) & 524287) | ((in[ 4 + inPos] & 31) << 14);
      out[ 7 + outPos] = ((in[ 4 + inPos] >>>  5) & 524287);
      out[ 8 + outPos] = ((in[ 4 + inPos] >>> 24) & 524287) | ((in[ 5 + inPos] & 2047) <<  8);
      out[ 9 + outPos] = ((in[ 5 + inPos] >>> 11) & 524287);
      out[10 + outPos] = ((in[ 5 + inPos] >>> 30) & 524287) | ((in[ 6 + inPos] & 131071) <<  2);
      out[11 + outPos] = ((in[ 6 + inPos] >>> 17) & 524287) | ((in[ 7 + inPos] & 15) << 15);
      out[12 + outPos] = ((in[ 7 + inPos] >>>  4) & 524287);
      out[13 + outPos] = ((in[ 7 + inPos] >>> 23) & 524287) | ((in[ 8 + inPos] & 1023) <<  9);
      out[14 + outPos] = ((in[ 8 + inPos] >>> 10) & 524287);
      out[15 + outPos] = ((in[ 8 + inPos] >>> 29) & 524287) | ((in[ 9 + inPos] & 65535) <<  3);
      out[16 + outPos] = ((in[ 9 + inPos] >>> 16) & 524287) | ((in[10 + inPos] & 7) << 16);
      out[17 + outPos] = ((in[10 + inPos] >>>  3) & 524287);
      out[18 + outPos] = ((in[10 + inPos] >>> 22) & 524287) | ((in[11 + inPos] & 511) << 10);
      out[19 + outPos] = ((in[11 + inPos] >>>  9) & 524287);
      out[20 + outPos] = ((in[11 + inPos] >>> 28) & 524287) | ((in[12 + inPos] & 32767) <<  4);
      out[21 + outPos] = ((in[12 + inPos] >>> 15) & 524287) | ((in[13 + inPos] & 3) << 17);
      out[22 + outPos] = ((in[13 + inPos] >>>  2) & 524287);
      out[23 + outPos] = ((in[13 + inPos] >>> 21) & 524287) | ((in[14 + inPos] & 255) << 11);
      out[24 + outPos] = ((in[14 + inPos] >>>  8) & 524287);
      out[25 + outPos] = ((in[14 + inPos] >>> 27) & 524287) | ((in[15 + inPos] & 16383) <<  5);
      out[26 + outPos] = ((in[15 + inPos] >>> 14) & 524287) | ((in[16 + inPos] & 1) << 18);
      out[27 + outPos] = ((in[16 + inPos] >>>  1) & 524287);
      out[28 + outPos] = ((in[16 + inPos] >>> 20) & 524287) | ((in[17 + inPos] & 127) << 12);
      out[29 + outPos] = ((in[17 + inPos] >>>  7) & 524287);
      out[30 + outPos] = ((in[17 + inPos] >>> 26) & 524287) | ((in[18 + inPos] & 8191) <<  6);
      out[31 + outPos] = ((in[18 + inPos] >>> 13) & 524287);
    }
  }

  private static final class Packer20 extends IntPacker {

    private Packer20() {
      super(20);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 1048575) <<   0)
        | ((in[ 1 + inPos] & 1048575) <<  20);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 1048575) >>> 12)
        | ((in[ 2 + inPos] & 1048575) <<   8)
        | ((in[ 3 + inPos] & 1048575) <<  28);
      out[ 2 + outPos] =
          ((in[ 3 + inPos] & 1048575) >>>  4)
        | ((in[ 4 + inPos] & 1048575) <<  16);
      out[ 3 + outPos] =
          ((in[ 4 + inPos] & 1048575) >>> 16)
        | ((in[ 5 + inPos] & 1048575) <<   4)
        | ((in[ 6 + inPos] & 1048575) <<  24);
      out[ 4 + outPos] =
          ((in[ 6 + inPos] & 1048575) >>>  8)
        | ((in[ 7 + inPos] & 1048575) <<  12);
      out[ 5 + outPos] =
          ((in[ 8 + inPos] & 1048575) <<   0)
        | ((in[ 9 + inPos] & 1048575) <<  20);
      out[ 6 + outPos] =
          ((in[ 9 + inPos] & 1048575) >>> 12)
        | ((in[10 + inPos] & 1048575) <<   8)
        | ((in[11 + inPos] & 1048575) <<  28);
      out[ 7 + outPos] =
          ((in[11 + inPos] & 1048575) >>>  4)
        | ((in[12 + inPos] & 1048575) <<  16);
      out[ 8 + outPos] =
          ((in[12 + inPos] & 1048575) >>> 16)
        | ((in[13 + inPos] & 1048575) <<   4)
        | ((in[14 + inPos] & 1048575) <<  24);
      out[ 9 + outPos] =
          ((in[14 + inPos] & 1048575) >>>  8)
        | ((in[15 + inPos] & 1048575) <<  12);
      out[10 + outPos] =
          ((in[16 + inPos] & 1048575) <<   0)
        | ((in[17 + inPos] & 1048575) <<  20);
      out[11 + outPos] =
          ((in[17 + inPos] & 1048575) >>> 12)
        | ((in[18 + inPos] & 1048575) <<   8)
        | ((in[19 + inPos] & 1048575) <<  28);
      out[12 + outPos] =
          ((in[19 + inPos] & 1048575) >>>  4)
        | ((in[20 + inPos] & 1048575) <<  16);
      out[13 + outPos] =
          ((in[20 + inPos] & 1048575) >>> 16)
        | ((in[21 + inPos] & 1048575) <<   4)
        | ((in[22 + inPos] & 1048575) <<  24);
      out[14 + outPos] =
          ((in[22 + inPos] & 1048575) >>>  8)
        | ((in[23 + inPos] & 1048575) <<  12);
      out[15 + outPos] =
          ((in[24 + inPos] & 1048575) <<   0)
        | ((in[25 + inPos] & 1048575) <<  20);
      out[16 + outPos] =
          ((in[25 + inPos] & 1048575) >>> 12)
        | ((in[26 + inPos] & 1048575) <<   8)
        | ((in[27 + inPos] & 1048575) <<  28);
      out[17 + outPos] =
          ((in[27 + inPos] & 1048575) >>>  4)
        | ((in[28 + inPos] & 1048575) <<  16);
      out[18 + outPos] =
          ((in[28 + inPos] & 1048575) >>> 16)
        | ((in[29 + inPos] & 1048575) <<   4)
        | ((in[30 + inPos] & 1048575) <<  24);
      out[19 + outPos] =
          ((in[30 + inPos] & 1048575) >>>  8)
        | ((in[31 + inPos] & 1048575) <<  12);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 1048575);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 20) & 1048575) | ((in[ 1 + inPos] & 255) << 12);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>>  8) & 1048575);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 28) & 1048575) | ((in[ 2 + inPos] & 65535) <<  4);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>> 16) & 1048575) | ((in[ 3 + inPos] & 15) << 16);
      out[ 5 + outPos] = ((in[ 3 + inPos] >>>  4) & 1048575);
      out[ 6 + outPos] = ((in[ 3 + inPos] >>> 24) & 1048575) | ((in[ 4 + inPos] & 4095) <<  8);
      out[ 7 + outPos] = ((in[ 4 + inPos] >>> 12) & 1048575);
      out[ 8 + outPos] = ((in[ 5 + inPos] >>>  0) & 1048575);
      out[ 9 + outPos] = ((in[ 5 + inPos] >>> 20) & 1048575) | ((in[ 6 + inPos] & 255) << 12);
      out[10 + outPos] = ((in[ 6 + inPos] >>>  8) & 1048575);
      out[11 + outPos] = ((in[ 6 + inPos] >>> 28) & 1048575) | ((in[ 7 + inPos] & 65535) <<  4);
      out[12 + outPos] = ((in[ 7 + inPos] >>> 16) & 1048575) | ((in[ 8 + inPos] & 15) << 16);
      out[13 + outPos] = ((in[ 8 + inPos] >>>  4) & 1048575);
      out[14 + outPos] = ((in[ 8 + inPos] >>> 24) & 1048575) | ((in[ 9 + inPos] & 4095) <<  8);
      out[15 + outPos] = ((in[ 9 + inPos] >>> 12) & 1048575);
      out[16 + outPos] = ((in[10 + inPos] >>>  0) & 1048575);
      out[17 + outPos] = ((in[10 + inPos] >>> 20) & 1048575) | ((in[11 + inPos] & 255) << 12);
      out[18 + outPos] = ((in[11 + inPos] >>>  8) & 1048575);
      out[19 + outPos] = ((in[11 + inPos] >>> 28) & 1048575) | ((in[12 + inPos] & 65535) <<  4);
      out[20 + outPos] = ((in[12 + inPos] >>> 16) & 1048575) | ((in[13 + inPos] & 15) << 16);
      out[21 + outPos] = ((in[13 + inPos] >>>  4) & 1048575);
      out[22 + outPos] = ((in[13 + inPos] >>> 24) & 1048575) | ((in[14 + inPos] & 4095) <<  8);
      out[23 + outPos] = ((in[14 + inPos] >>> 12) & 1048575);
      out[24 + outPos] = ((in[15 + inPos] >>>  0) & 1048575);
      out[25 + outPos] = ((in[15 + inPos] >>> 20) & 1048575) | ((in[16 + inPos] & 255) << 12);
      out[26 + outPos] = ((in[16 + inPos] >>>  8) & 1048575);
      out[27 + outPos] = ((in[16 + inPos] >>> 28) & 1048575) | ((in[17 + inPos] & 65535) <<  4);
      out[28 + outPos] = ((in[17 + inPos] >>> 16) & 1048575) | ((in[18 + inPos] & 15) << 16);
      out[29 + outPos] = ((in[18 + inPos] >>>  4) & 1048575);
      out[30 + outPos] = ((in[18 + inPos] >>> 24) & 1048575) | ((in[19 + inPos] & 4095) <<  8);
      out[31 + outPos] = ((in[19 + inPos] >>> 12) & 1048575);
    }
  }

  private static final class Packer21 extends IntPacker {

    private Packer21() {
      super(21);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 2097151) <<   0)
        | ((in[ 1 + inPos] & 2097151) <<  21);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 2097151) >>> 11)
        | ((in[ 2 + inPos] & 2097151) <<  10)
        | ((in[ 3 + inPos] & 2097151) <<  31);
      out[ 2 + outPos] =
          ((in[ 3 + inPos] & 2097151) >>>  1)
        | ((in[ 4 + inPos] & 2097151) <<  20);
      out[ 3 + outPos] =
          ((in[ 4 + inPos] & 2097151) >>> 12)
        | ((in[ 5 + inPos] & 2097151) <<   9)
        | ((in[ 6 + inPos] & 2097151) <<  30);
      out[ 4 + outPos] =
          ((in[ 6 + inPos] & 2097151) >>>  2)
        | ((in[ 7 + inPos] & 2097151) <<  19);
      out[ 5 + outPos] =
          ((in[ 7 + inPos] & 2097151) >>> 13)
        | ((in[ 8 + inPos] & 2097151) <<   8)
        | ((in[ 9 + inPos] & 2097151) <<  29);
      out[ 6 + outPos] =
          ((in[ 9 + inPos] & 2097151) >>>  3)
        | ((in[10 + inPos] & 2097151) <<  18);
      out[ 7 + outPos] =
          ((in[10 + inPos] & 2097151) >>> 14)
        | ((in[11 + inPos] & 2097151) <<   7)
        | ((in[12 + inPos] & 2097151) <<  28);
      out[ 8 + outPos] =
          ((in[12 + inPos] & 2097151) >>>  4)
        | ((in[13 + inPos] & 2097151) <<  17);
      out[ 9 + outPos] =
          ((in[13 + inPos] & 2097151) >>> 15)
        | ((in[14 + inPos] & 2097151) <<   6)
        | ((in[15 + inPos] & 2097151) <<  27);
      out[10 + outPos] =
          ((in[15 + inPos] & 2097151) >>>  5)
        | ((in[16 + inPos] & 2097151) <<  16);
      out[11 + outPos] =
          ((in[16 + inPos] & 2097151) >>> 16)
        | ((in[17 + inPos] & 2097151) <<   5)
        | ((in[18 + inPos] & 2097151) <<  26);
      out[12 + outPos] =
          ((in[18 + inPos] & 2097151) >>>  6)
        | ((in[19 + inPos] & 2097151) <<  15);
      out[13 + outPos] =
          ((in[19 + inPos] & 2097151) >>> 17)
        | ((in[20 + inPos] & 2097151) <<   4)
        | ((in[21 + inPos] & 2097151) <<  25);
      out[14 + outPos] =
          ((in[21 + inPos] & 2097151) >>>  7)
        | ((in[22 + inPos] & 2097151) <<  14);
      out[15 + outPos] =
          ((in[22 + inPos] & 2097151) >>> 18)
        | ((in[23 + inPos] & 2097151) <<   3)
        | ((in[24 + inPos] & 2097151) <<  24);
      out[16 + outPos] =
          ((in[24 + inPos] & 2097151) >>>  8)
        | ((in[25 + inPos] & 2097151) <<  13);
      out[17 + outPos] =
          ((in[25 + inPos] & 2097151) >>> 19)
        | ((in[26 + inPos] & 2097151) <<   2)
        | ((in[27 + inPos] & 2097151) <<  23);
      out[18 + outPos] =
          ((in[27 + inPos] & 2097151) >>>  9)
        | ((in[28 + inPos] & 2097151) <<  12);
      out[19 + outPos] =
          ((in[28 + inPos] & 2097151) >>> 20)
        | ((in[29 + inPos] & 2097151) <<   1)
        | ((in[30 + inPos] & 2097151) <<  22);
      out[20 + outPos] =
          ((in[30 + inPos] & 2097151) >>> 10)
        | ((in[31 + inPos] & 2097151) <<  11);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 2097151);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 21) & 2097151) | ((in[ 1 + inPos] & 1023) << 11);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 10) & 2097151);
      out[ 3 + outPos] = ((in[ 1 + inPos] >>> 31) & 2097151) | ((in[ 2 + inPos] & 1048575) <<  1);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>> 20) & 2097151) | ((in[ 3 + inPos] & 511) << 12);
      out[ 5 + outPos] = ((in[ 3 + inPos] >>>  9) & 2097151);
      out[ 6 + outPos] = ((in[ 3 + inPos] >>> 30) & 2097151) | ((in[ 4 + inPos] & 524287) <<  2);
      out[ 7 + outPos] = ((in[ 4 + inPos] >>> 19) & 2097151) | ((in[ 5 + inPos] & 255) << 13);
      out[ 8 + outPos] = ((in[ 5 + inPos] >>>  8) & 2097151);
      out[ 9 + outPos] = ((in[ 5 + inPos] >>> 29) & 2097151) | ((in[ 6 + inPos] & 262143) <<  3);
      out[10 + outPos] = ((in[ 6 + inPos] >>> 18) & 2097151) | ((in[ 7 + inPos] & 127) << 14);
      out[11 + outPos] = ((in[ 7 + inPos] >>>  7) & 2097151);
      out[12 + outPos] = ((in[ 7 + inPos] >>> 28) & 2097151) | ((in[ 8 + inPos] & 131071) <<  4);
      out[13 + outPos] = ((in[ 8 + inPos] >>> 17) & 2097151) | ((in[ 9 + inPos] & 63) << 15);
      out[14 + outPos] = ((in[ 9 + inPos] >>>  6) & 2097151);
      out[15 + outPos] = ((in[ 9 + inPos] >>> 27) & 2097151) | ((in[10 + inPos] & 65535) <<  5);
      out[16 + outPos] = ((in[10 + inPos] >>> 16) & 2097151) | ((in[11 + inPos] & 31) << 16);
      out[17 + outPos] = ((in[11 + inPos] >>>  5) & 2097151);
      out[18 + outPos] = ((in[11 + inPos] >>> 26) & 2097151) | ((in[12 + inPos] & 32767) <<  6);
      out[19 + outPos] = ((in[12 + inPos] >>> 15) & 2097151) | ((in[13 + inPos] & 15) << 17);
      out[20 + outPos] = ((in[13 + inPos] >>>  4) & 2097151);
      out[21 + outPos] = ((in[13 + inPos] >>> 25) & 2097151) | ((in[14 + inPos] & 16383) <<  7);
      out[22 + outPos] = ((in[14 + inPos] >>> 14) & 2097151) | ((in[15 + inPos] & 7) << 18);
      out[23 + outPos] = ((in[15 + inPos] >>>  3) & 2097151);
      out[24 + outPos] = ((in[15 + inPos] >>> 24) & 2097151) | ((in[16 + inPos] & 8191) <<  8);
      out[25 + outPos] = ((in[16 + inPos] >>> 13) & 2097151) | ((in[17 + inPos] & 3) << 19);
      out[26 + outPos] = ((in[17 + inPos] >>>  2) & 2097151);
      out[27 + outPos] = ((in[17 + inPos] >>> 23) & 2097151) | ((in[18 + inPos] & 4095) <<  9);
      out[28 + outPos] = ((in[18 + inPos] >>> 12) & 2097151) | ((in[19 + inPos] & 1) << 20);
      out[29 + outPos] = ((in[19 + inPos] >>>  1) & 2097151);
      out[30 + outPos] = ((in[19 + inPos] >>> 22) & 2097151) | ((in[20 + inPos] & 2047) << 10);
      out[31 + outPos] = ((in[20 + inPos] >>> 11) & 2097151);
    }
  }

  private static final class Packer22 extends IntPacker {

    private Packer22() {
      super(22);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 4194303) <<   0)
        | ((in[ 1 + inPos] & 4194303) <<  22);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 4194303) >>> 10)
        | ((in[ 2 + inPos] & 4194303) <<  12);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 4194303) >>> 20)
        | ((in[ 3 + inPos] & 4194303) <<   2)
        | ((in[ 4 + inPos] & 4194303) <<  24);
      out[ 3 + outPos] =
          ((in[ 4 + inPos] & 4194303) >>>  8)
        | ((in[ 5 + inPos] & 4194303) <<  14);
      out[ 4 + outPos] =
          ((in[ 5 + inPos] & 4194303) >>> 18)
        | ((in[ 6 + inPos] & 4194303) <<   4)
        | ((in[ 7 + inPos] & 4194303) <<  26);
      out[ 5 + outPos] =
          ((in[ 7 + inPos] & 4194303) >>>  6)
        | ((in[ 8 + inPos] & 4194303) <<  16);
      out[ 6 + outPos] =
          ((in[ 8 + inPos] & 4194303) >>> 16)
        | ((in[ 9 + inPos] & 4194303) <<   6)
        | ((in[10 + inPos] & 4194303) <<  28);
      out[ 7 + outPos] =
          ((in[10 + inPos] & 4194303) >>>  4)
        | ((in[11 + inPos] & 4194303) <<  18);
      out[ 8 + outPos] =
          ((in[11 + inPos] & 4194303) >>> 14)
        | ((in[12 + inPos] & 4194303) <<   8)
        | ((in[13 + inPos] & 4194303) <<  30);
      out[ 9 + outPos] =
          ((in[13 + inPos] & 4194303) >>>  2)
        | ((in[14 + inPos] & 4194303) <<  20);
      out[10 + outPos] =
          ((in[14 + inPos] & 4194303) >>> 12)
        | ((in[15 + inPos] & 4194303) <<  10);
      out[11 + outPos] =
          ((in[16 + inPos] & 4194303) <<   0)
        | ((in[17 + inPos] & 4194303) <<  22);
      out[12 + outPos] =
          ((in[17 + inPos] & 4194303) >>> 10)
        | ((in[18 + inPos] & 4194303) <<  12);
      out[13 + outPos] =
          ((in[18 + inPos] & 4194303) >>> 20)
        | ((in[19 + inPos] & 4194303) <<   2)
        | ((in[20 + inPos] & 4194303) <<  24);
      out[14 + outPos] =
          ((in[20 + inPos] & 4194303) >>>  8)
        | ((in[21 + inPos] & 4194303) <<  14);
      out[15 + outPos] =
          ((in[21 + inPos] & 4194303) >>> 18)
        | ((in[22 + inPos] & 4194303) <<   4)
        | ((in[23 + inPos] & 4194303) <<  26);
      out[16 + outPos] =
          ((in[23 + inPos] & 4194303) >>>  6)
        | ((in[24 + inPos] & 4194303) <<  16);
      out[17 + outPos] =
          ((in[24 + inPos] & 4194303) >>> 16)
        | ((in[25 + inPos] & 4194303) <<   6)
        | ((in[26 + inPos] & 4194303) <<  28);
      out[18 + outPos] =
          ((in[26 + inPos] & 4194303) >>>  4)
        | ((in[27 + inPos] & 4194303) <<  18);
      out[19 + outPos] =
          ((in[27 + inPos] & 4194303) >>> 14)
        | ((in[28 + inPos] & 4194303) <<   8)
        | ((in[29 + inPos] & 4194303) <<  30);
      out[20 + outPos] =
          ((in[29 + inPos] & 4194303) >>>  2)
        | ((in[30 + inPos] & 4194303) <<  20);
      out[21 + outPos] =
          ((in[30 + inPos] & 4194303) >>> 12)
        | ((in[31 + inPos] & 4194303) <<  10);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 4194303);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 22) & 4194303) | ((in[ 1 + inPos] & 4095) << 10);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 12) & 4194303) | ((in[ 2 + inPos] & 3) << 20);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>>  2) & 4194303);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>> 24) & 4194303) | ((in[ 3 + inPos] & 16383) <<  8);
      out[ 5 + outPos] = ((in[ 3 + inPos] >>> 14) & 4194303) | ((in[ 4 + inPos] & 15) << 18);
      out[ 6 + outPos] = ((in[ 4 + inPos] >>>  4) & 4194303);
      out[ 7 + outPos] = ((in[ 4 + inPos] >>> 26) & 4194303) | ((in[ 5 + inPos] & 65535) <<  6);
      out[ 8 + outPos] = ((in[ 5 + inPos] >>> 16) & 4194303) | ((in[ 6 + inPos] & 63) << 16);
      out[ 9 + outPos] = ((in[ 6 + inPos] >>>  6) & 4194303);
      out[10 + outPos] = ((in[ 6 + inPos] >>> 28) & 4194303) | ((in[ 7 + inPos] & 262143) <<  4);
      out[11 + outPos] = ((in[ 7 + inPos] >>> 18) & 4194303) | ((in[ 8 + inPos] & 255) << 14);
      out[12 + outPos] = ((in[ 8 + inPos] >>>  8) & 4194303);
      out[13 + outPos] = ((in[ 8 + inPos] >>> 30) & 4194303) | ((in[ 9 + inPos] & 1048575) <<  2);
      out[14 + outPos] = ((in[ 9 + inPos] >>> 20) & 4194303) | ((in[10 + inPos] & 1023) << 12);
      out[15 + outPos] = ((in[10 + inPos] >>> 10) & 4194303);
      out[16 + outPos] = ((in[11 + inPos] >>>  0) & 4194303);
      out[17 + outPos] = ((in[11 + inPos] >>> 22) & 4194303) | ((in[12 + inPos] & 4095) << 10);
      out[18 + outPos] = ((in[12 + inPos] >>> 12) & 4194303) | ((in[13 + inPos] & 3) << 20);
      out[19 + outPos] = ((in[13 + inPos] >>>  2) & 4194303);
      out[20 + outPos] = ((in[13 + inPos] >>> 24) & 4194303) | ((in[14 + inPos] & 16383) <<  8);
      out[21 + outPos] = ((in[14 + inPos] >>> 14) & 4194303) | ((in[15 + inPos] & 15) << 18);
      out[22 + outPos] = ((in[15 + inPos] >>>  4) & 4194303);
      out[23 + outPos] = ((in[15 + inPos] >>> 26) & 4194303) | ((in[16 + inPos] & 65535) <<  6);
      out[24 + outPos] = ((in[16 + inPos] >>> 16) & 4194303) | ((in[17 + inPos] & 63) << 16);
      out[25 + outPos] = ((in[17 + inPos] >>>  6) & 4194303);
      out[26 + outPos] = ((in[17 + inPos] >>> 28) & 4194303) | ((in[18 + inPos] & 262143) <<  4);
      out[27 + outPos] = ((in[18 + inPos] >>> 18) & 4194303) | ((in[19 + inPos] & 255) << 14);
      out[28 + outPos] = ((in[19 + inPos] >>>  8) & 4194303);
      out[29 + outPos] = ((in[19 + inPos] >>> 30) & 4194303) | ((in[20 + inPos] & 1048575) <<  2);
      out[30 + outPos] = ((in[20 + inPos] >>> 20) & 4194303) | ((in[21 + inPos] & 1023) << 12);
      out[31 + outPos] = ((in[21 + inPos] >>> 10) & 4194303);
    }
  }

  private static final class Packer23 extends IntPacker {

    private Packer23() {
      super(23);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 8388607) <<   0)
        | ((in[ 1 + inPos] & 8388607) <<  23);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 8388607) >>>  9)
        | ((in[ 2 + inPos] & 8388607) <<  14);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 8388607) >>> 18)
        | ((in[ 3 + inPos] & 8388607) <<   5)
        | ((in[ 4 + inPos] & 8388607) <<  28);
      out[ 3 + outPos] =
          ((in[ 4 + inPos] & 8388607) >>>  4)
        | ((in[ 5 + inPos] & 8388607) <<  19);
      out[ 4 + outPos] =
          ((in[ 5 + inPos] & 8388607) >>> 13)
        | ((in[ 6 + inPos] & 8388607) <<  10);
      out[ 5 + outPos] =
          ((in[ 6 + inPos] & 8388607) >>> 22)
        | ((in[ 7 + inPos] & 8388607) <<   1)
        | ((in[ 8 + inPos] & 8388607) <<  24);
      out[ 6 + outPos] =
          ((in[ 8 + inPos] & 8388607) >>>  8)
        | ((in[ 9 + inPos] & 8388607) <<  15);
      out[ 7 + outPos] =
          ((in[ 9 + inPos] & 8388607) >>> 17)
        | ((in[10 + inPos] & 8388607) <<   6)
        | ((in[11 + inPos] & 8388607) <<  29);
      out[ 8 + outPos] =
          ((in[11 + inPos] & 8388607) >>>  3)
        | ((in[12 + inPos] & 8388607) <<  20);
      out[ 9 + outPos] =
          ((in[12 + inPos] & 8388607) >>> 12)
        | ((in[13 + inPos] & 8388607) <<  11);
      out[10 + outPos] =
          ((in[13 + inPos] & 8388607) >>> 21)
        | ((in[14 + inPos] & 8388607) <<   2)
        | ((in[15 + inPos] & 8388607) <<  25);
      out[11 + outPos] =
          ((in[15 + inPos] & 8388607) >>>  7)
        | ((in[16 + inPos] & 8388607) <<  16);
      out[12 + outPos] =
          ((in[16 + inPos] & 8388607) >>> 16)
        | ((in[17 + inPos] & 8388607) <<   7)
        | ((in[18 + inPos] & 8388607) <<  30);
      out[13 + outPos] =
          ((in[18 + inPos] & 8388607) >>>  2)
        | ((in[19 + inPos] & 8388607) <<  21);
      out[14 + outPos] =
          ((in[19 + inPos] & 8388607) >>> 11)
        | ((in[20 + inPos] & 8388607) <<  12);
      out[15 + outPos] =
          ((in[20 + inPos] & 8388607) >>> 20)
        | ((in[21 + inPos] & 8388607) <<   3)
        | ((in[22 + inPos] & 8388607) <<  26);
      out[16 + outPos] =
          ((in[22 + inPos] & 8388607) >>>  6)
        | ((in[23 + inPos] & 8388607) <<  17);
      out[17 + outPos] =
          ((in[23 + inPos] & 8388607) >>> 15)
        | ((in[24 + inPos] & 8388607) <<   8)
        | ((in[25 + inPos] & 8388607) <<  31);
      out[18 + outPos] =
          ((in[25 + inPos] & 8388607) >>>  1)
        | ((in[26 + inPos] & 8388607) <<  22);
      out[19 + outPos] =
          ((in[26 + inPos] & 8388607) >>> 10)
        | ((in[27 + inPos] & 8388607) <<  13);
      out[20 + outPos] =
          ((in[27 + inPos] & 8388607) >>> 19)
        | ((in[28 + inPos] & 8388607) <<   4)
        | ((in[29 + inPos] & 8388607) <<  27);
      out[21 + outPos] =
          ((in[29 + inPos] & 8388607) >>>  5)
        | ((in[30 + inPos] & 8388607) <<  18);
      out[22 + outPos] =
          ((in[30 + inPos] & 8388607) >>> 14)
        | ((in[31 + inPos] & 8388607) <<   9);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 8388607);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 23) & 8388607) | ((in[ 1 + inPos] & 16383) <<  9);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 14) & 8388607) | ((in[ 2 + inPos] & 31) << 18);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>>  5) & 8388607);
      out[ 4 + outPos] = ((in[ 2 + inPos] >>> 28) & 8388607) | ((in[ 3 + inPos] & 524287) <<  4);
      out[ 5 + outPos] = ((in[ 3 + inPos] >>> 19) & 8388607) | ((in[ 4 + inPos] & 1023) << 13);
      out[ 6 + outPos] = ((in[ 4 + inPos] >>> 10) & 8388607) | ((in[ 5 + inPos] & 1) << 22);
      out[ 7 + outPos] = ((in[ 5 + inPos] >>>  1) & 8388607);
      out[ 8 + outPos] = ((in[ 5 + inPos] >>> 24) & 8388607) | ((in[ 6 + inPos] & 32767) <<  8);
      out[ 9 + outPos] = ((in[ 6 + inPos] >>> 15) & 8388607) | ((in[ 7 + inPos] & 63) << 17);
      out[10 + outPos] = ((in[ 7 + inPos] >>>  6) & 8388607);
      out[11 + outPos] = ((in[ 7 + inPos] >>> 29) & 8388607) | ((in[ 8 + inPos] & 1048575) <<  3);
      out[12 + outPos] = ((in[ 8 + inPos] >>> 20) & 8388607) | ((in[ 9 + inPos] & 2047) << 12);
      out[13 + outPos] = ((in[ 9 + inPos] >>> 11) & 8388607) | ((in[10 + inPos] & 3) << 21);
      out[14 + outPos] = ((in[10 + inPos] >>>  2) & 8388607);
      out[15 + outPos] = ((in[10 + inPos] >>> 25) & 8388607) | ((in[11 + inPos] & 65535) <<  7);
      out[16 + outPos] = ((in[11 + inPos] >>> 16) & 8388607) | ((in[12 + inPos] & 127) << 16);
      out[17 + outPos] = ((in[12 + inPos] >>>  7) & 8388607);
      out[18 + outPos] = ((in[12 + inPos] >>> 30) & 8388607) | ((in[13 + inPos] & 2097151) <<  2);
      out[19 + outPos] = ((in[13 + inPos] >>> 21) & 8388607) | ((in[14 + inPos] & 4095) << 11);
      out[20 + outPos] = ((in[14 + inPos] >>> 12) & 8388607) | ((in[15 + inPos] & 7) << 20);
      out[21 + outPos] = ((in[15 + inPos] >>>  3) & 8388607);
      out[22 + outPos] = ((in[15 + inPos] >>> 26) & 8388607) | ((in[16 + inPos] & 131071) <<  6);
      out[23 + outPos] = ((in[16 + inPos] >>> 17) & 8388607) | ((in[17 + inPos] & 255) << 15);
      out[24 + outPos] = ((in[17 + inPos] >>>  8) & 8388607);
      out[25 + outPos] = ((in[17 + inPos] >>> 31) & 8388607) | ((in[18 + inPos] & 4194303) <<  1);
      out[26 + outPos] = ((in[18 + inPos] >>> 22) & 8388607) | ((in[19 + inPos] & 8191) << 10);
      out[27 + outPos] = ((in[19 + inPos] >>> 13) & 8388607) | ((in[20 + inPos] & 15) << 19);
      out[28 + outPos] = ((in[20 + inPos] >>>  4) & 8388607);
      out[29 + outPos] = ((in[20 + inPos] >>> 27) & 8388607) | ((in[21 + inPos] & 262143) <<  5);
      out[30 + outPos] = ((in[21 + inPos] >>> 18) & 8388607) | ((in[22 + inPos] & 511) << 14);
      out[31 + outPos] = ((in[22 + inPos] >>>  9) & 8388607);
    }
  }

  private static final class Packer24 extends IntPacker {

    private Packer24() {
      super(24);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 16777215) <<   0)
        | ((in[ 1 + inPos] & 16777215) <<  24);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 16777215) >>>  8)
        | ((in[ 2 + inPos] & 16777215) <<  16);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 16777215) >>> 16)
        | ((in[ 3 + inPos] & 16777215) <<   8);
      out[ 3 + outPos] =
          ((in[ 4 + inPos] & 16777215) <<   0)
        | ((in[ 5 + inPos] & 16777215) <<  24);
      out[ 4 + outPos] =
          ((in[ 5 + inPos] & 16777215) >>>  8)
        | ((in[ 6 + inPos] & 16777215) <<  16);
      out[ 5 + outPos] =
          ((in[ 6 + inPos] & 16777215) >>> 16)
        | ((in[ 7 + inPos] & 16777215) <<   8);
      out[ 6 + outPos] =
          ((in[ 8 + inPos] & 16777215) <<   0)
        | ((in[ 9 + inPos] & 16777215) <<  24);
      out[ 7 + outPos] =
          ((in[ 9 + inPos] & 16777215) >>>  8)
        | ((in[10 + inPos] & 16777215) <<  16);
      out[ 8 + outPos] =
          ((in[10 + inPos] & 16777215) >>> 16)
        | ((in[11 + inPos] & 16777215) <<   8);
      out[ 9 + outPos] =
          ((in[12 + inPos] & 16777215) <<   0)
        | ((in[13 + inPos] & 16777215) <<  24);
      out[10 + outPos] =
          ((in[13 + inPos] & 16777215) >>>  8)
        | ((in[14 + inPos] & 16777215) <<  16);
      out[11 + outPos] =
          ((in[14 + inPos] & 16777215) >>> 16)
        | ((in[15 + inPos] & 16777215) <<   8);
      out[12 + outPos] =
          ((in[16 + inPos] & 16777215) <<   0)
        | ((in[17 + inPos] & 16777215) <<  24);
      out[13 + outPos] =
          ((in[17 + inPos] & 16777215) >>>  8)
        | ((in[18 + inPos] & 16777215) <<  16);
      out[14 + outPos] =
          ((in[18 + inPos] & 16777215) >>> 16)
        | ((in[19 + inPos] & 16777215) <<   8);
      out[15 + outPos] =
          ((in[20 + inPos] & 16777215) <<   0)
        | ((in[21 + inPos] & 16777215) <<  24);
      out[16 + outPos] =
          ((in[21 + inPos] & 16777215) >>>  8)
        | ((in[22 + inPos] & 16777215) <<  16);
      out[17 + outPos] =
          ((in[22 + inPos] & 16777215) >>> 16)
        | ((in[23 + inPos] & 16777215) <<   8);
      out[18 + outPos] =
          ((in[24 + inPos] & 16777215) <<   0)
        | ((in[25 + inPos] & 16777215) <<  24);
      out[19 + outPos] =
          ((in[25 + inPos] & 16777215) >>>  8)
        | ((in[26 + inPos] & 16777215) <<  16);
      out[20 + outPos] =
          ((in[26 + inPos] & 16777215) >>> 16)
        | ((in[27 + inPos] & 16777215) <<   8);
      out[21 + outPos] =
          ((in[28 + inPos] & 16777215) <<   0)
        | ((in[29 + inPos] & 16777215) <<  24);
      out[22 + outPos] =
          ((in[29 + inPos] & 16777215) >>>  8)
        | ((in[30 + inPos] & 16777215) <<  16);
      out[23 + outPos] =
          ((in[30 + inPos] & 16777215) >>> 16)
        | ((in[31 + inPos] & 16777215) <<   8);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 16777215);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 24) & 16777215) | ((in[ 1 + inPos] & 65535) <<  8);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 16) & 16777215) | ((in[ 2 + inPos] & 255) << 16);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>>  8) & 16777215);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>>  0) & 16777215);
      out[ 5 + outPos] = ((in[ 3 + inPos] >>> 24) & 16777215) | ((in[ 4 + inPos] & 65535) <<  8);
      out[ 6 + outPos] = ((in[ 4 + inPos] >>> 16) & 16777215) | ((in[ 5 + inPos] & 255) << 16);
      out[ 7 + outPos] = ((in[ 5 + inPos] >>>  8) & 16777215);
      out[ 8 + outPos] = ((in[ 6 + inPos] >>>  0) & 16777215);
      out[ 9 + outPos] = ((in[ 6 + inPos] >>> 24) & 16777215) | ((in[ 7 + inPos] & 65535) <<  8);
      out[10 + outPos] = ((in[ 7 + inPos] >>> 16) & 16777215) | ((in[ 8 + inPos] & 255) << 16);
      out[11 + outPos] = ((in[ 8 + inPos] >>>  8) & 16777215);
      out[12 + outPos] = ((in[ 9 + inPos] >>>  0) & 16777215);
      out[13 + outPos] = ((in[ 9 + inPos] >>> 24) & 16777215) | ((in[10 + inPos] & 65535) <<  8);
      out[14 + outPos] = ((in[10 + inPos] >>> 16) & 16777215) | ((in[11 + inPos] & 255) << 16);
      out[15 + outPos] = ((in[11 + inPos] >>>  8) & 16777215);
      out[16 + outPos] = ((in[12 + inPos] >>>  0) & 16777215);
      out[17 + outPos] = ((in[12 + inPos] >>> 24) & 16777215) | ((in[13 + inPos] & 65535) <<  8);
      out[18 + outPos] = ((in[13 + inPos] >>> 16) & 16777215) | ((in[14 + inPos] & 255) << 16);
      out[19 + outPos] = ((in[14 + inPos] >>>  8) & 16777215);
      out[20 + outPos] = ((in[15 + inPos] >>>  0) & 16777215);
      out[21 + outPos] = ((in[15 + inPos] >>> 24) & 16777215) | ((in[16 + inPos] & 65535) <<  8);
      out[22 + outPos] = ((in[16 + inPos] >>> 16) & 16777215) | ((in[17 + inPos] & 255) << 16);
      out[23 + outPos] = ((in[17 + inPos] >>>  8) & 16777215);
      out[24 + outPos] = ((in[18 + inPos] >>>  0) & 16777215);
      out[25 + outPos] = ((in[18 + inPos] >>> 24) & 16777215) | ((in[19 + inPos] & 65535) <<  8);
      out[26 + outPos] = ((in[19 + inPos] >>> 16) & 16777215) | ((in[20 + inPos] & 255) << 16);
      out[27 + outPos] = ((in[20 + inPos] >>>  8) & 16777215);
      out[28 + outPos] = ((in[21 + inPos] >>>  0) & 16777215);
      out[29 + outPos] = ((in[21 + inPos] >>> 24) & 16777215) | ((in[22 + inPos] & 65535) <<  8);
      out[30 + outPos] = ((in[22 + inPos] >>> 16) & 16777215) | ((in[23 + inPos] & 255) << 16);
      out[31 + outPos] = ((in[23 + inPos] >>>  8) & 16777215);
    }
  }

  private static final class Packer25 extends IntPacker {

    private Packer25() {
      super(25);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 33554431) <<   0)
        | ((in[ 1 + inPos] & 33554431) <<  25);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 33554431) >>>  7)
        | ((in[ 2 + inPos] & 33554431) <<  18);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 33554431) >>> 14)
        | ((in[ 3 + inPos] & 33554431) <<  11);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 33554431) >>> 21)
        | ((in[ 4 + inPos] & 33554431) <<   4)
        | ((in[ 5 + inPos] & 33554431) <<  29);
      out[ 4 + outPos] =
          ((in[ 5 + inPos] & 33554431) >>>  3)
        | ((in[ 6 + inPos] & 33554431) <<  22);
      out[ 5 + outPos] =
          ((in[ 6 + inPos] & 33554431) >>> 10)
        | ((in[ 7 + inPos] & 33554431) <<  15);
      out[ 6 + outPos] =
          ((in[ 7 + inPos] & 33554431) >>> 17)
        | ((in[ 8 + inPos] & 33554431) <<   8);
      out[ 7 + outPos] =
          ((in[ 8 + inPos] & 33554431) >>> 24)
        | ((in[ 9 + inPos] & 33554431) <<   1)
        | ((in[10 + inPos] & 33554431) <<  26);
      out[ 8 + outPos] =
          ((in[10 + inPos] & 33554431) >>>  6)
        | ((in[11 + inPos] & 33554431) <<  19);
      out[ 9 + outPos] =
          ((in[11 + inPos] & 33554431) >>> 13)
        | ((in[12 + inPos] & 33554431) <<  12);
      out[10 + outPos] =
          ((in[12 + inPos] & 33554431) >>> 20)
        | ((in[13 + inPos] & 33554431) <<   5)
        | ((in[14 + inPos] & 33554431) <<  30);
      out[11 + outPos] =
          ((in[14 + inPos] & 33554431) >>>  2)
        | ((in[15 + inPos] & 33554431) <<  23);
      out[12 + outPos] =
          ((in[15 + inPos] & 33554431) >>>  9)
        | ((in[16 + inPos] & 33554431) <<  16);
      out[13 + outPos] =
          ((in[16 + inPos] & 33554431) >>> 16)
        | ((in[17 + inPos] & 33554431) <<   9);
      out[14 + outPos] =
          ((in[17 + inPos] & 33554431) >>> 23)
        | ((in[18 + inPos] & 33554431) <<   2)
        | ((in[19 + inPos] & 33554431) <<  27);
      out[15 + outPos] =
          ((in[19 + inPos] & 33554431) >>>  5)
        | ((in[20 + inPos] & 33554431) <<  20);
      out[16 + outPos] =
          ((in[20 + inPos] & 33554431) >>> 12)
        | ((in[21 + inPos] & 33554431) <<  13);
      out[17 + outPos] =
          ((in[21 + inPos] & 33554431) >>> 19)
        | ((in[22 + inPos] & 33554431) <<   6)
        | ((in[23 + inPos] & 33554431) <<  31);
      out[18 + outPos] =
          ((in[23 + inPos] & 33554431) >>>  1)
        | ((in[24 + inPos] & 33554431) <<  24);
      out[19 + outPos] =
          ((in[24 + inPos] & 33554431) >>>  8)
        | ((in[25 + inPos] & 33554431) <<  17);
      out[20 + outPos] =
          ((in[25 + inPos] & 33554431) >>> 15)
        | ((in[26 + inPos] & 33554431) <<  10);
      out[21 + outPos] =
          ((in[26 + inPos] & 33554431) >>> 22)
        | ((in[27 + inPos] & 33554431) <<   3)
        | ((in[28 + inPos] & 33554431) <<  28);
      out[22 + outPos] =
          ((in[28 + inPos] & 33554431) >>>  4)
        | ((in[29 + inPos] & 33554431) <<  21);
      out[23 + outPos] =
          ((in[29 + inPos] & 33554431) >>> 11)
        | ((in[30 + inPos] & 33554431) <<  14);
      out[24 + outPos] =
          ((in[30 + inPos] & 33554431) >>> 18)
        | ((in[31 + inPos] & 33554431) <<   7);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 33554431);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 25) & 33554431) | ((in[ 1 + inPos] & 262143) <<  7);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 18) & 33554431) | ((in[ 2 + inPos] & 2047) << 14);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 11) & 33554431) | ((in[ 3 + inPos] & 15) << 21);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>>  4) & 33554431);
      out[ 5 + outPos] = ((in[ 3 + inPos] >>> 29) & 33554431) | ((in[ 4 + inPos] & 4194303) <<  3);
      out[ 6 + outPos] = ((in[ 4 + inPos] >>> 22) & 33554431) | ((in[ 5 + inPos] & 32767) << 10);
      out[ 7 + outPos] = ((in[ 5 + inPos] >>> 15) & 33554431) | ((in[ 6 + inPos] & 255) << 17);
      out[ 8 + outPos] = ((in[ 6 + inPos] >>>  8) & 33554431) | ((in[ 7 + inPos] & 1) << 24);
      out[ 9 + outPos] = ((in[ 7 + inPos] >>>  1) & 33554431);
      out[10 + outPos] = ((in[ 7 + inPos] >>> 26) & 33554431) | ((in[ 8 + inPos] & 524287) <<  6);
      out[11 + outPos] = ((in[ 8 + inPos] >>> 19) & 33554431) | ((in[ 9 + inPos] & 4095) << 13);
      out[12 + outPos] = ((in[ 9 + inPos] >>> 12) & 33554431) | ((in[10 + inPos] & 31) << 20);
      out[13 + outPos] = ((in[10 + inPos] >>>  5) & 33554431);
      out[14 + outPos] = ((in[10 + inPos] >>> 30) & 33554431) | ((in[11 + inPos] & 8388607) <<  2);
      out[15 + outPos] = ((in[11 + inPos] >>> 23) & 33554431) | ((in[12 + inPos] & 65535) <<  9);
      out[16 + outPos] = ((in[12 + inPos] >>> 16) & 33554431) | ((in[13 + inPos] & 511) << 16);
      out[17 + outPos] = ((in[13 + inPos] >>>  9) & 33554431) | ((in[14 + inPos] & 3) << 23);
      out[18 + outPos] = ((in[14 + inPos] >>>  2) & 33554431);
      out[19 + outPos] = ((in[14 + inPos] >>> 27) & 33554431) | ((in[15 + inPos] & 1048575) <<  5);
      out[20 + outPos] = ((in[15 + inPos] >>> 20) & 33554431) | ((in[16 + inPos] & 8191) << 12);
      out[21 + outPos] = ((in[16 + inPos] >>> 13) & 33554431) | ((in[17 + inPos] & 63) << 19);
      out[22 + outPos] = ((in[17 + inPos] >>>  6) & 33554431);
      out[23 + outPos] = ((in[17 + inPos] >>> 31) & 33554431) | ((in[18 + inPos] & 16777215) <<  1);
      out[24 + outPos] = ((in[18 + inPos] >>> 24) & 33554431) | ((in[19 + inPos] & 131071) <<  8);
      out[25 + outPos] = ((in[19 + inPos] >>> 17) & 33554431) | ((in[20 + inPos] & 1023) << 15);
      out[26 + outPos] = ((in[20 + inPos] >>> 10) & 33554431) | ((in[21 + inPos] & 7) << 22);
      out[27 + outPos] = ((in[21 + inPos] >>>  3) & 33554431);
      out[28 + outPos] = ((in[21 + inPos] >>> 28) & 33554431) | ((in[22 + inPos] & 2097151) <<  4);
      out[29 + outPos] = ((in[22 + inPos] >>> 21) & 33554431) | ((in[23 + inPos] & 16383) << 11);
      out[30 + outPos] = ((in[23 + inPos] >>> 14) & 33554431) | ((in[24 + inPos] & 127) << 18);
      out[31 + outPos] = ((in[24 + inPos] >>>  7) & 33554431);
    }
  }

  private static final class Packer26 extends IntPacker {

    private Packer26() {
      super(26);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 67108863) <<   0)
        | ((in[ 1 + inPos] & 67108863) <<  26);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 67108863) >>>  6)
        | ((in[ 2 + inPos] & 67108863) <<  20);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 67108863) >>> 12)
        | ((in[ 3 + inPos] & 67108863) <<  14);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 67108863) >>> 18)
        | ((in[ 4 + inPos] & 67108863) <<   8);
      out[ 4 + outPos] =
          ((in[ 4 + inPos] & 67108863) >>> 24)
        | ((in[ 5 + inPos] & 67108863) <<   2)
        | ((in[ 6 + inPos] & 67108863) <<  28);
      out[ 5 + outPos] =
          ((in[ 6 + inPos] & 67108863) >>>  4)
        | ((in[ 7 + inPos] & 67108863) <<  22);
      out[ 6 + outPos] =
          ((in[ 7 + inPos] & 67108863) >>> 10)
        | ((in[ 8 + inPos] & 67108863) <<  16);
      out[ 7 + outPos] =
          ((in[ 8 + inPos] & 67108863) >>> 16)
        | ((in[ 9 + inPos] & 67108863) <<  10);
      out[ 8 + outPos] =
          ((in[ 9 + inPos] & 67108863) >>> 22)
        | ((in[10 + inPos] & 67108863) <<   4)
        | ((in[11 + inPos] & 67108863) <<  30);
      out[ 9 + outPos] =
          ((in[11 + inPos] & 67108863) >>>  2)
        | ((in[12 + inPos] & 67108863) <<  24);
      out[10 + outPos] =
          ((in[12 + inPos] & 67108863) >>>  8)
        | ((in[13 + inPos] & 67108863) <<  18);
      out[11 + outPos] =
          ((in[13 + inPos] & 67108863) >>> 14)
        | ((in[14 + inPos] & 67108863) <<  12);
      out[12 + outPos] =
          ((in[14 + inPos] & 67108863) >>> 20)
        | ((in[15 + inPos] & 67108863) <<   6);
      out[13 + outPos] =
          ((in[16 + inPos] & 67108863) <<   0)
        | ((in[17 + inPos] & 67108863) <<  26);
      out[14 + outPos] =
          ((in[17 + inPos] & 67108863) >>>  6)
        | ((in[18 + inPos] & 67108863) <<  20);
      out[15 + outPos] =
          ((in[18 + inPos] & 67108863) >>> 12)
        | ((in[19 + inPos] & 67108863) <<  14);
      out[16 + outPos] =
          ((in[19 + inPos] & 67108863) >>> 18)
        | ((in[20 + inPos] & 67108863) <<   8);
      out[17 + outPos] =
          ((in[20 + inPos] & 67108863) >>> 24)
        | ((in[21 + inPos] & 67108863) <<   2)
        | ((in[22 + inPos] & 67108863) <<  28);
      out[18 + outPos] =
          ((in[22 + inPos] & 67108863) >>>  4)
        | ((in[23 + inPos] & 67108863) <<  22);
      out[19 + outPos] =
          ((in[23 + inPos] & 67108863) >>> 10)
        | ((in[24 + inPos] & 67108863) <<  16);
      out[20 + outPos] =
          ((in[24 + inPos] & 67108863) >>> 16)
        | ((in[25 + inPos] & 67108863) <<  10);
      out[21 + outPos] =
          ((in[25 + inPos] & 67108863) >>> 22)
        | ((in[26 + inPos] & 67108863) <<   4)
        | ((in[27 + inPos] & 67108863) <<  30);
      out[22 + outPos] =
          ((in[27 + inPos] & 67108863) >>>  2)
        | ((in[28 + inPos] & 67108863) <<  24);
      out[23 + outPos] =
          ((in[28 + inPos] & 67108863) >>>  8)
        | ((in[29 + inPos] & 67108863) <<  18);
      out[24 + outPos] =
          ((in[29 + inPos] & 67108863) >>> 14)
        | ((in[30 + inPos] & 67108863) <<  12);
      out[25 + outPos] =
          ((in[30 + inPos] & 67108863) >>> 20)
        | ((in[31 + inPos] & 67108863) <<   6);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 67108863);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 26) & 67108863) | ((in[ 1 + inPos] & 1048575) <<  6);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 20) & 67108863) | ((in[ 2 + inPos] & 16383) << 12);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 14) & 67108863) | ((in[ 3 + inPos] & 255) << 18);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>>  8) & 67108863) | ((in[ 4 + inPos] & 3) << 24);
      out[ 5 + outPos] = ((in[ 4 + inPos] >>>  2) & 67108863);
      out[ 6 + outPos] = ((in[ 4 + inPos] >>> 28) & 67108863) | ((in[ 5 + inPos] & 4194303) <<  4);
      out[ 7 + outPos] = ((in[ 5 + inPos] >>> 22) & 67108863) | ((in[ 6 + inPos] & 65535) << 10);
      out[ 8 + outPos] = ((in[ 6 + inPos] >>> 16) & 67108863) | ((in[ 7 + inPos] & 1023) << 16);
      out[ 9 + outPos] = ((in[ 7 + inPos] >>> 10) & 67108863) | ((in[ 8 + inPos] & 15) << 22);
      out[10 + outPos] = ((in[ 8 + inPos] >>>  4) & 67108863);
      out[11 + outPos] = ((in[ 8 + inPos] >>> 30) & 67108863) | ((in[ 9 + inPos] & 16777215) <<  2);
      out[12 + outPos] = ((in[ 9 + inPos] >>> 24) & 67108863) | ((in[10 + inPos] & 262143) <<  8);
      out[13 + outPos] = ((in[10 + inPos] >>> 18) & 67108863) | ((in[11 + inPos] & 4095) << 14);
      out[14 + outPos] = ((in[11 + inPos] >>> 12) & 67108863) | ((in[12 + inPos] & 63) << 20);
      out[15 + outPos] = ((in[12 + inPos] >>>  6) & 67108863);
      out[16 + outPos] = ((in[13 + inPos] >>>  0) & 67108863);
      out[17 + outPos] = ((in[13 + inPos] >>> 26) & 67108863) | ((in[14 + inPos] & 1048575) <<  6);
      out[18 + outPos] = ((in[14 + inPos] >>> 20) & 67108863) | ((in[15 + inPos] & 16383) << 12);
      out[19 + outPos] = ((in[15 + inPos] >>> 14) & 67108863) | ((in[16 + inPos] & 255) << 18);
      out[20 + outPos] = ((in[16 + inPos] >>>  8) & 67108863) | ((in[17 + inPos] & 3) << 24);
      out[21 + outPos] = ((in[17 + inPos] >>>  2) & 67108863);
      out[22 + outPos] = ((in[17 + inPos] >>> 28) & 67108863) | ((in[18 + inPos] & 4194303) <<  4);
      out[23 + outPos] = ((in[18 + inPos] >>> 22) & 67108863) | ((in[19 + inPos] & 65535) << 10);
      out[24 + outPos] = ((in[19 + inPos] >>> 16) & 67108863) | ((in[20 + inPos] & 1023) << 16);
      out[25 + outPos] = ((in[20 + inPos] >>> 10) & 67108863) | ((in[21 + inPos] & 15) << 22);
      out[26 + outPos] = ((in[21 + inPos] >>>  4) & 67108863);
      out[27 + outPos] = ((in[21 + inPos] >>> 30) & 67108863) | ((in[22 + inPos] & 16777215) <<  2);
      out[28 + outPos] = ((in[22 + inPos] >>> 24) & 67108863) | ((in[23 + inPos] & 262143) <<  8);
      out[29 + outPos] = ((in[23 + inPos] >>> 18) & 67108863) | ((in[24 + inPos] & 4095) << 14);
      out[30 + outPos] = ((in[24 + inPos] >>> 12) & 67108863) | ((in[25 + inPos] & 63) << 20);
      out[31 + outPos] = ((in[25 + inPos] >>>  6) & 67108863);
    }
  }

  private static final class Packer27 extends IntPacker {

    private Packer27() {
      super(27);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 134217727) <<   0)
        | ((in[ 1 + inPos] & 134217727) <<  27);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 134217727) >>>  5)
        | ((in[ 2 + inPos] & 134217727) <<  22);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 134217727) >>> 10)
        | ((in[ 3 + inPos] & 134217727) <<  17);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 134217727) >>> 15)
        | ((in[ 4 + inPos] & 134217727) <<  12);
      out[ 4 + outPos] =
          ((in[ 4 + inPos] & 134217727) >>> 20)
        | ((in[ 5 + inPos] & 134217727) <<   7);
      out[ 5 + outPos] =
          ((in[ 5 + inPos] & 134217727) >>> 25)
        | ((in[ 6 + inPos] & 134217727) <<   2)
        | ((in[ 7 + inPos] & 134217727) <<  29);
      out[ 6 + outPos] =
          ((in[ 7 + inPos] & 134217727) >>>  3)
        | ((in[ 8 + inPos] & 134217727) <<  24);
      out[ 7 + outPos] =
          ((in[ 8 + inPos] & 134217727) >>>  8)
        | ((in[ 9 + inPos] & 134217727) <<  19);
      out[ 8 + outPos] =
          ((in[ 9 + inPos] & 134217727) >>> 13)
        | ((in[10 + inPos] & 134217727) <<  14);
      out[ 9 + outPos] =
          ((in[10 + inPos] & 134217727) >>> 18)
        | ((in[11 + inPos] & 134217727) <<   9);
      out[10 + outPos] =
          ((in[11 + inPos] & 134217727) >>> 23)
        | ((in[12 + inPos] & 134217727) <<   4)
        | ((in[13 + inPos] & 134217727) <<  31);
      out[11 + outPos] =
          ((in[13 + inPos] & 134217727) >>>  1)
        | ((in[14 + inPos] & 134217727) <<  26);
      out[12 + outPos] =
          ((in[14 + inPos] & 134217727) >>>  6)
        | ((in[15 + inPos] & 134217727) <<  21);
      out[13 + outPos] =
          ((in[15 + inPos] & 134217727) >>> 11)
        | ((in[16 + inPos] & 134217727) <<  16);
      out[14 + outPos] =
          ((in[16 + inPos] & 134217727) >>> 16)
        | ((in[17 + inPos] & 134217727) <<  11);
      out[15 + outPos] =
          ((in[17 + inPos] & 134217727) >>> 21)
        | ((in[18 + inPos] & 134217727) <<   6);
      out[16 + outPos] =
          ((in[18 + inPos] & 134217727) >>> 26)
        | ((in[19 + inPos] & 134217727) <<   1)
        | ((in[20 + inPos] & 134217727) <<  28);
      out[17 + outPos] =
          ((in[20 + inPos] & 134217727) >>>  4)
        | ((in[21 + inPos] & 134217727) <<  23);
      out[18 + outPos] =
          ((in[21 + inPos] & 134217727) >>>  9)
        | ((in[22 + inPos] & 134217727) <<  18);
      out[19 + outPos] =
          ((in[22 + inPos] & 134217727) >>> 14)
        | ((in[23 + inPos] & 134217727) <<  13);
      out[20 + outPos] =
          ((in[23 + inPos] & 134217727) >>> 19)
        | ((in[24 + inPos] & 134217727) <<   8);
      out[21 + outPos] =
          ((in[24 + inPos] & 134217727) >>> 24)
        | ((in[25 + inPos] & 134217727) <<   3)
        | ((in[26 + inPos] & 134217727) <<  30);
      out[22 + outPos] =
          ((in[26 + inPos] & 134217727) >>>  2)
        | ((in[27 + inPos] & 134217727) <<  25);
      out[23 + outPos] =
          ((in[27 + inPos] & 134217727) >>>  7)
        | ((in[28 + inPos] & 134217727) <<  20);
      out[24 + outPos] =
          ((in[28 + inPos] & 134217727) >>> 12)
        | ((in[29 + inPos] & 134217727) <<  15);
      out[25 + outPos] =
          ((in[29 + inPos] & 134217727) >>> 17)
        | ((in[30 + inPos] & 134217727) <<  10);
      out[26 + outPos] =
          ((in[30 + inPos] & 134217727) >>> 22)
        | ((in[31 + inPos] & 134217727) <<   5);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 134217727);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 27) & 134217727) | ((in[ 1 + inPos] & 4194303) <<  5);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 22) & 134217727) | ((in[ 2 + inPos] & 131071) << 10);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 17) & 134217727) | ((in[ 3 + inPos] & 4095) << 15);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>> 12) & 134217727) | ((in[ 4 + inPos] & 127) << 20);
      out[ 5 + outPos] = ((in[ 4 + inPos] >>>  7) & 134217727) | ((in[ 5 + inPos] & 3) << 25);
      out[ 6 + outPos] = ((in[ 5 + inPos] >>>  2) & 134217727);
      out[ 7 + outPos] = ((in[ 5 + inPos] >>> 29) & 134217727) | ((in[ 6 + inPos] & 16777215) <<  3);
      out[ 8 + outPos] = ((in[ 6 + inPos] >>> 24) & 134217727) | ((in[ 7 + inPos] & 524287) <<  8);
      out[ 9 + outPos] = ((in[ 7 + inPos] >>> 19) & 134217727) | ((in[ 8 + inPos] & 16383) << 13);
      out[10 + outPos] = ((in[ 8 + inPos] >>> 14) & 134217727) | ((in[ 9 + inPos] & 511) << 18);
      out[11 + outPos] = ((in[ 9 + inPos] >>>  9) & 134217727) | ((in[10 + inPos] & 15) << 23);
      out[12 + outPos] = ((in[10 + inPos] >>>  4) & 134217727);
      out[13 + outPos] = ((in[10 + inPos] >>> 31) & 134217727) | ((in[11 + inPos] & 67108863) <<  1);
      out[14 + outPos] = ((in[11 + inPos] >>> 26) & 134217727) | ((in[12 + inPos] & 2097151) <<  6);
      out[15 + outPos] = ((in[12 + inPos] >>> 21) & 134217727) | ((in[13 + inPos] & 65535) << 11);
      out[16 + outPos] = ((in[13 + inPos] >>> 16) & 134217727) | ((in[14 + inPos] & 2047) << 16);
      out[17 + outPos] = ((in[14 + inPos] >>> 11) & 134217727) | ((in[15 + inPos] & 63) << 21);
      out[18 + outPos] = ((in[15 + inPos] >>>  6) & 134217727) | ((in[16 + inPos] & 1) << 26);
      out[19 + outPos] = ((in[16 + inPos] >>>  1) & 134217727);
      out[20 + outPos] = ((in[16 + inPos] >>> 28) & 134217727) | ((in[17 + inPos] & 8388607) <<  4);
      out[21 + outPos] = ((in[17 + inPos] >>> 23) & 134217727) | ((in[18 + inPos] & 262143) <<  9);
      out[22 + outPos] = ((in[18 + inPos] >>> 18) & 134217727) | ((in[19 + inPos] & 8191) << 14);
      out[23 + outPos] = ((in[19 + inPos] >>> 13) & 134217727) | ((in[20 + inPos] & 255) << 19);
      out[24 + outPos] = ((in[20 + inPos] >>>  8) & 134217727) | ((in[21 + inPos] & 7) << 24);
      out[25 + outPos] = ((in[21 + inPos] >>>  3) & 134217727);
      out[26 + outPos] = ((in[21 + inPos] >>> 30) & 134217727) | ((in[22 + inPos] & 33554431) <<  2);
      out[27 + outPos] = ((in[22 + inPos] >>> 25) & 134217727) | ((in[23 + inPos] & 1048575) <<  7);
      out[28 + outPos] = ((in[23 + inPos] >>> 20) & 134217727) | ((in[24 + inPos] & 32767) << 12);
      out[29 + outPos] = ((in[24 + inPos] >>> 15) & 134217727) | ((in[25 + inPos] & 1023) << 17);
      out[30 + outPos] = ((in[25 + inPos] >>> 10) & 134217727) | ((in[26 + inPos] & 31) << 22);
      out[31 + outPos] = ((in[26 + inPos] >>>  5) & 134217727);
    }
  }

  private static final class Packer28 extends IntPacker {

    private Packer28() {
      super(28);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 268435455) <<   0)
        | ((in[ 1 + inPos] & 268435455) <<  28);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 268435455) >>>  4)
        | ((in[ 2 + inPos] & 268435455) <<  24);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 268435455) >>>  8)
        | ((in[ 3 + inPos] & 268435455) <<  20);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 268435455) >>> 12)
        | ((in[ 4 + inPos] & 268435455) <<  16);
      out[ 4 + outPos] =
          ((in[ 4 + inPos] & 268435455) >>> 16)
        | ((in[ 5 + inPos] & 268435455) <<  12);
      out[ 5 + outPos] =
          ((in[ 5 + inPos] & 268435455) >>> 20)
        | ((in[ 6 + inPos] & 268435455) <<   8);
      out[ 6 + outPos] =
          ((in[ 6 + inPos] & 268435455) >>> 24)
        | ((in[ 7 + inPos] & 268435455) <<   4);
      out[ 7 + outPos] =
          ((in[ 8 + inPos] & 268435455) <<   0)
        | ((in[ 9 + inPos] & 268435455) <<  28);
      out[ 8 + outPos] =
          ((in[ 9 + inPos] & 268435455) >>>  4)
        | ((in[10 + inPos] & 268435455) <<  24);
      out[ 9 + outPos] =
          ((in[10 + inPos] & 268435455) >>>  8)
        | ((in[11 + inPos] & 268435455) <<  20);
      out[10 + outPos] =
          ((in[11 + inPos] & 268435455) >>> 12)
        | ((in[12 + inPos] & 268435455) <<  16);
      out[11 + outPos] =
          ((in[12 + inPos] & 268435455) >>> 16)
        | ((in[13 + inPos] & 268435455) <<  12);
      out[12 + outPos] =
          ((in[13 + inPos] & 268435455) >>> 20)
        | ((in[14 + inPos] & 268435455) <<   8);
      out[13 + outPos] =
          ((in[14 + inPos] & 268435455) >>> 24)
        | ((in[15 + inPos] & 268435455) <<   4);
      out[14 + outPos] =
          ((in[16 + inPos] & 268435455) <<   0)
        | ((in[17 + inPos] & 268435455) <<  28);
      out[15 + outPos] =
          ((in[17 + inPos] & 268435455) >>>  4)
        | ((in[18 + inPos] & 268435455) <<  24);
      out[16 + outPos] =
          ((in[18 + inPos] & 268435455) >>>  8)
        | ((in[19 + inPos] & 268435455) <<  20);
      out[17 + outPos] =
          ((in[19 + inPos] & 268435455) >>> 12)
        | ((in[20 + inPos] & 268435455) <<  16);
      out[18 + outPos] =
          ((in[20 + inPos] & 268435455) >>> 16)
        | ((in[21 + inPos] & 268435455) <<  12);
      out[19 + outPos] =
          ((in[21 + inPos] & 268435455) >>> 20)
        | ((in[22 + inPos] & 268435455) <<   8);
      out[20 + outPos] =
          ((in[22 + inPos] & 268435455) >>> 24)
        | ((in[23 + inPos] & 268435455) <<   4);
      out[21 + outPos] =
          ((in[24 + inPos] & 268435455) <<   0)
        | ((in[25 + inPos] & 268435455) <<  28);
      out[22 + outPos] =
          ((in[25 + inPos] & 268435455) >>>  4)
        | ((in[26 + inPos] & 268435455) <<  24);
      out[23 + outPos] =
          ((in[26 + inPos] & 268435455) >>>  8)
        | ((in[27 + inPos] & 268435455) <<  20);
      out[24 + outPos] =
          ((in[27 + inPos] & 268435455) >>> 12)
        | ((in[28 + inPos] & 268435455) <<  16);
      out[25 + outPos] =
          ((in[28 + inPos] & 268435455) >>> 16)
        | ((in[29 + inPos] & 268435455) <<  12);
      out[26 + outPos] =
          ((in[29 + inPos] & 268435455) >>> 20)
        | ((in[30 + inPos] & 268435455) <<   8);
      out[27 + outPos] =
          ((in[30 + inPos] & 268435455) >>> 24)
        | ((in[31 + inPos] & 268435455) <<   4);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 268435455);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 28) & 268435455) | ((in[ 1 + inPos] & 16777215) <<  4);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 24) & 268435455) | ((in[ 2 + inPos] & 1048575) <<  8);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 20) & 268435455) | ((in[ 3 + inPos] & 65535) << 12);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>> 16) & 268435455) | ((in[ 4 + inPos] & 4095) << 16);
      out[ 5 + outPos] = ((in[ 4 + inPos] >>> 12) & 268435455) | ((in[ 5 + inPos] & 255) << 20);
      out[ 6 + outPos] = ((in[ 5 + inPos] >>>  8) & 268435455) | ((in[ 6 + inPos] & 15) << 24);
      out[ 7 + outPos] = ((in[ 6 + inPos] >>>  4) & 268435455);
      out[ 8 + outPos] = ((in[ 7 + inPos] >>>  0) & 268435455);
      out[ 9 + outPos] = ((in[ 7 + inPos] >>> 28) & 268435455) | ((in[ 8 + inPos] & 16777215) <<  4);
      out[10 + outPos] = ((in[ 8 + inPos] >>> 24) & 268435455) | ((in[ 9 + inPos] & 1048575) <<  8);
      out[11 + outPos] = ((in[ 9 + inPos] >>> 20) & 268435455) | ((in[10 + inPos] & 65535) << 12);
      out[12 + outPos] = ((in[10 + inPos] >>> 16) & 268435455) | ((in[11 + inPos] & 4095) << 16);
      out[13 + outPos] = ((in[11 + inPos] >>> 12) & 268435455) | ((in[12 + inPos] & 255) << 20);
      out[14 + outPos] = ((in[12 + inPos] >>>  8) & 268435455) | ((in[13 + inPos] & 15) << 24);
      out[15 + outPos] = ((in[13 + inPos] >>>  4) & 268435455);
      out[16 + outPos] = ((in[14 + inPos] >>>  0) & 268435455);
      out[17 + outPos] = ((in[14 + inPos] >>> 28) & 268435455) | ((in[15 + inPos] & 16777215) <<  4);
      out[18 + outPos] = ((in[15 + inPos] >>> 24) & 268435455) | ((in[16 + inPos] & 1048575) <<  8);
      out[19 + outPos] = ((in[16 + inPos] >>> 20) & 268435455) | ((in[17 + inPos] & 65535) << 12);
      out[20 + outPos] = ((in[17 + inPos] >>> 16) & 268435455) | ((in[18 + inPos] & 4095) << 16);
      out[21 + outPos] = ((in[18 + inPos] >>> 12) & 268435455) | ((in[19 + inPos] & 255) << 20);
      out[22 + outPos] = ((in[19 + inPos] >>>  8) & 268435455) | ((in[20 + inPos] & 15) << 24);
      out[23 + outPos] = ((in[20 + inPos] >>>  4) & 268435455);
      out[24 + outPos] = ((in[21 + inPos] >>>  0) & 268435455);
      out[25 + outPos] = ((in[21 + inPos] >>> 28) & 268435455) | ((in[22 + inPos] & 16777215) <<  4);
      out[26 + outPos] = ((in[22 + inPos] >>> 24) & 268435455) | ((in[23 + inPos] & 1048575) <<  8);
      out[27 + outPos] = ((in[23 + inPos] >>> 20) & 268435455) | ((in[24 + inPos] & 65535) << 12);
      out[28 + outPos] = ((in[24 + inPos] >>> 16) & 268435455) | ((in[25 + inPos] & 4095) << 16);
      out[29 + outPos] = ((in[25 + inPos] >>> 12) & 268435455) | ((in[26 + inPos] & 255) << 20);
      out[30 + outPos] = ((in[26 + inPos] >>>  8) & 268435455) | ((in[27 + inPos] & 15) << 24);
      out[31 + outPos] = ((in[27 + inPos] >>>  4) & 268435455);
    }
  }

  private static final class Packer29 extends IntPacker {

    private Packer29() {
      super(29);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 536870911) <<   0)
        | ((in[ 1 + inPos] & 536870911) <<  29);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 536870911) >>>  3)
        | ((in[ 2 + inPos] & 536870911) <<  26);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 536870911) >>>  6)
        | ((in[ 3 + inPos] & 536870911) <<  23);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 536870911) >>>  9)
        | ((in[ 4 + inPos] & 536870911) <<  20);
      out[ 4 + outPos] =
          ((in[ 4 + inPos] & 536870911) >>> 12)
        | ((in[ 5 + inPos] & 536870911) <<  17);
      out[ 5 + outPos] =
          ((in[ 5 + inPos] & 536870911) >>> 15)
        | ((in[ 6 + inPos] & 536870911) <<  14);
      out[ 6 + outPos] =
          ((in[ 6 + inPos] & 536870911) >>> 18)
        | ((in[ 7 + inPos] & 536870911) <<  11);
      out[ 7 + outPos] =
          ((in[ 7 + inPos] & 536870911) >>> 21)
        | ((in[ 8 + inPos] & 536870911) <<   8);
      out[ 8 + outPos] =
          ((in[ 8 + inPos] & 536870911) >>> 24)
        | ((in[ 9 + inPos] & 536870911) <<   5);
      out[ 9 + outPos] =
          ((in[ 9 + inPos] & 536870911) >>> 27)
        | ((in[10 + inPos] & 536870911) <<   2)
        | ((in[11 + inPos] & 536870911) <<  31);
      out[10 + outPos] =
          ((in[11 + inPos] & 536870911) >>>  1)
        | ((in[12 + inPos] & 536870911) <<  28);
      out[11 + outPos] =
          ((in[12 + inPos] & 536870911) >>>  4)
        | ((in[13 + inPos] & 536870911) <<  25);
      out[12 + outPos] =
          ((in[13 + inPos] & 536870911) >>>  7)
        | ((in[14 + inPos] & 536870911) <<  22);
      out[13 + outPos] =
          ((in[14 + inPos] & 536870911) >>> 10)
        | ((in[15 + inPos] & 536870911) <<  19);
      out[14 + outPos] =
          ((in[15 + inPos] & 536870911) >>> 13)
        | ((in[16 + inPos] & 536870911) <<  16);
      out[15 + outPos] =
          ((in[16 + inPos] & 536870911) >>> 16)
        | ((in[17 + inPos] & 536870911) <<  13);
      out[16 + outPos] =
          ((in[17 + inPos] & 536870911) >>> 19)
        | ((in[18 + inPos] & 536870911) <<  10);
      out[17 + outPos] =
          ((in[18 + inPos] & 536870911) >>> 22)
        | ((in[19 + inPos] & 536870911) <<   7);
      out[18 + outPos] =
          ((in[19 + inPos] & 536870911) >>> 25)
        | ((in[20 + inPos] & 536870911) <<   4);
      out[19 + outPos] =
          ((in[20 + inPos] & 536870911) >>> 28)
        | ((in[21 + inPos] & 536870911) <<   1)
        | ((in[22 + inPos] & 536870911) <<  30);
      out[20 + outPos] =
          ((in[22 + inPos] & 536870911) >>>  2)
        | ((in[23 + inPos] & 536870911) <<  27);
      out[21 + outPos] =
          ((in[23 + inPos] & 536870911) >>>  5)
        | ((in[24 + inPos] & 536870911) <<  24);
      out[22 + outPos] =
          ((in[24 + inPos] & 536870911) >>>  8)
        | ((in[25 + inPos] & 536870911) <<  21);
      out[23 + outPos] =
          ((in[25 + inPos] & 536870911) >>> 11)
        | ((in[26 + inPos] & 536870911) <<  18);
      out[24 + outPos] =
          ((in[26 + inPos] & 536870911) >>> 14)
        | ((in[27 + inPos] & 536870911) <<  15);
      out[25 + outPos] =
          ((in[27 + inPos] & 536870911) >>> 17)
        | ((in[28 + inPos] & 536870911) <<  12);
      out[26 + outPos] =
          ((in[28 + inPos] & 536870911) >>> 20)
        | ((in[29 + inPos] & 536870911) <<   9);
      out[27 + outPos] =
          ((in[29 + inPos] & 536870911) >>> 23)
        | ((in[30 + inPos] & 536870911) <<   6);
      out[28 + outPos] =
          ((in[30 + inPos] & 536870911) >>> 26)
        | ((in[31 + inPos] & 536870911) <<   3);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 536870911);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 29) & 536870911) | ((in[ 1 + inPos] & 67108863) <<  3);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 26) & 536870911) | ((in[ 2 + inPos] & 8388607) <<  6);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 23) & 536870911) | ((in[ 3 + inPos] & 1048575) <<  9);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>> 20) & 536870911) | ((in[ 4 + inPos] & 131071) << 12);
      out[ 5 + outPos] = ((in[ 4 + inPos] >>> 17) & 536870911) | ((in[ 5 + inPos] & 16383) << 15);
      out[ 6 + outPos] = ((in[ 5 + inPos] >>> 14) & 536870911) | ((in[ 6 + inPos] & 2047) << 18);
      out[ 7 + outPos] = ((in[ 6 + inPos] >>> 11) & 536870911) | ((in[ 7 + inPos] & 255) << 21);
      out[ 8 + outPos] = ((in[ 7 + inPos] >>>  8) & 536870911) | ((in[ 8 + inPos] & 31) << 24);
      out[ 9 + outPos] = ((in[ 8 + inPos] >>>  5) & 536870911) | ((in[ 9 + inPos] & 3) << 27);
      out[10 + outPos] = ((in[ 9 + inPos] >>>  2) & 536870911);
      out[11 + outPos] = ((in[ 9 + inPos] >>> 31) & 536870911) | ((in[10 + inPos] & 268435455) <<  1);
      out[12 + outPos] = ((in[10 + inPos] >>> 28) & 536870911) | ((in[11 + inPos] & 33554431) <<  4);
      out[13 + outPos] = ((in[11 + inPos] >>> 25) & 536870911) | ((in[12 + inPos] & 4194303) <<  7);
      out[14 + outPos] = ((in[12 + inPos] >>> 22) & 536870911) | ((in[13 + inPos] & 524287) << 10);
      out[15 + outPos] = ((in[13 + inPos] >>> 19) & 536870911) | ((in[14 + inPos] & 65535) << 13);
      out[16 + outPos] = ((in[14 + inPos] >>> 16) & 536870911) | ((in[15 + inPos] & 8191) << 16);
      out[17 + outPos] = ((in[15 + inPos] >>> 13) & 536870911) | ((in[16 + inPos] & 1023) << 19);
      out[18 + outPos] = ((in[16 + inPos] >>> 10) & 536870911) | ((in[17 + inPos] & 127) << 22);
      out[19 + outPos] = ((in[17 + inPos] >>>  7) & 536870911) | ((in[18 + inPos] & 15) << 25);
      out[20 + outPos] = ((in[18 + inPos] >>>  4) & 536870911) | ((in[19 + inPos] & 1) << 28);
      out[21 + outPos] = ((in[19 + inPos] >>>  1) & 536870911);
      out[22 + outPos] = ((in[19 + inPos] >>> 30) & 536870911) | ((in[20 + inPos] & 134217727) <<  2);
      out[23 + outPos] = ((in[20 + inPos] >>> 27) & 536870911) | ((in[21 + inPos] & 16777215) <<  5);
      out[24 + outPos] = ((in[21 + inPos] >>> 24) & 536870911) | ((in[22 + inPos] & 2097151) <<  8);
      out[25 + outPos] = ((in[22 + inPos] >>> 21) & 536870911) | ((in[23 + inPos] & 262143) << 11);
      out[26 + outPos] = ((in[23 + inPos] >>> 18) & 536870911) | ((in[24 + inPos] & 32767) << 14);
      out[27 + outPos] = ((in[24 + inPos] >>> 15) & 536870911) | ((in[25 + inPos] & 4095) << 17);
      out[28 + outPos] = ((in[25 + inPos] >>> 12) & 536870911) | ((in[26 + inPos] & 511) << 20);
      out[29 + outPos] = ((in[26 + inPos] >>>  9) & 536870911) | ((in[27 + inPos] & 63) << 23);
      out[30 + outPos] = ((in[27 + inPos] >>>  6) & 536870911) | ((in[28 + inPos] & 7) << 26);
      out[31 + outPos] = ((in[28 + inPos] >>>  3) & 536870911);
    }
  }

  private static final class Packer30 extends IntPacker {

    private Packer30() {
      super(30);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 1073741823) <<   0)
        | ((in[ 1 + inPos] & 1073741823) <<  30);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 1073741823) >>>  2)
        | ((in[ 2 + inPos] & 1073741823) <<  28);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 1073741823) >>>  4)
        | ((in[ 3 + inPos] & 1073741823) <<  26);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 1073741823) >>>  6)
        | ((in[ 4 + inPos] & 1073741823) <<  24);
      out[ 4 + outPos] =
          ((in[ 4 + inPos] & 1073741823) >>>  8)
        | ((in[ 5 + inPos] & 1073741823) <<  22);
      out[ 5 + outPos] =
          ((in[ 5 + inPos] & 1073741823) >>> 10)
        | ((in[ 6 + inPos] & 1073741823) <<  20);
      out[ 6 + outPos] =
          ((in[ 6 + inPos] & 1073741823) >>> 12)
        | ((in[ 7 + inPos] & 1073741823) <<  18);
      out[ 7 + outPos] =
          ((in[ 7 + inPos] & 1073741823) >>> 14)
        | ((in[ 8 + inPos] & 1073741823) <<  16);
      out[ 8 + outPos] =
          ((in[ 8 + inPos] & 1073741823) >>> 16)
        | ((in[ 9 + inPos] & 1073741823) <<  14);
      out[ 9 + outPos] =
          ((in[ 9 + inPos] & 1073741823) >>> 18)
        | ((in[10 + inPos] & 1073741823) <<  12);
      out[10 + outPos] =
          ((in[10 + inPos] & 1073741823) >>> 20)
        | ((in[11 + inPos] & 1073741823) <<  10);
      out[11 + outPos] =
          ((in[11 + inPos] & 1073741823) >>> 22)
        | ((in[12 + inPos] & 1073741823) <<   8);
      out[12 + outPos] =
          ((in[12 + inPos] & 1073741823) >>> 24)
        | ((in[13 + inPos] & 1073741823) <<   6);
      out[13 + outPos] =
          ((in[13 + inPos] & 1073741823) >>> 26)
        | ((in[14 + inPos] & 1073741823) <<   4);
      out[14 + outPos] =
          ((in[14 + inPos] & 1073741823) >>> 28)
        | ((in[15 + inPos] & 1073741823) <<   2);
      out[15 + outPos] =
          ((in[16 + inPos] & 1073741823) <<   0)
        | ((in[17 + inPos] & 1073741823) <<  30);
      out[16 + outPos] =
          ((in[17 + inPos] & 1073741823) >>>  2)
        | ((in[18 + inPos] & 1073741823) <<  28);
      out[17 + outPos] =
          ((in[18 + inPos] & 1073741823) >>>  4)
        | ((in[19 + inPos] & 1073741823) <<  26);
      out[18 + outPos] =
          ((in[19 + inPos] & 1073741823) >>>  6)
        | ((in[20 + inPos] & 1073741823) <<  24);
      out[19 + outPos] =
          ((in[20 + inPos] & 1073741823) >>>  8)
        | ((in[21 + inPos] & 1073741823) <<  22);
      out[20 + outPos] =
          ((in[21 + inPos] & 1073741823) >>> 10)
        | ((in[22 + inPos] & 1073741823) <<  20);
      out[21 + outPos] =
          ((in[22 + inPos] & 1073741823) >>> 12)
        | ((in[23 + inPos] & 1073741823) <<  18);
      out[22 + outPos] =
          ((in[23 + inPos] & 1073741823) >>> 14)
        | ((in[24 + inPos] & 1073741823) <<  16);
      out[23 + outPos] =
          ((in[24 + inPos] & 1073741823) >>> 16)
        | ((in[25 + inPos] & 1073741823) <<  14);
      out[24 + outPos] =
          ((in[25 + inPos] & 1073741823) >>> 18)
        | ((in[26 + inPos] & 1073741823) <<  12);
      out[25 + outPos] =
          ((in[26 + inPos] & 1073741823) >>> 20)
        | ((in[27 + inPos] & 1073741823) <<  10);
      out[26 + outPos] =
          ((in[27 + inPos] & 1073741823) >>> 22)
        | ((in[28 + inPos] & 1073741823) <<   8);
      out[27 + outPos] =
          ((in[28 + inPos] & 1073741823) >>> 24)
        | ((in[29 + inPos] & 1073741823) <<   6);
      out[28 + outPos] =
          ((in[29 + inPos] & 1073741823) >>> 26)
        | ((in[30 + inPos] & 1073741823) <<   4);
      out[29 + outPos] =
          ((in[30 + inPos] & 1073741823) >>> 28)
        | ((in[31 + inPos] & 1073741823) <<   2);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 1073741823);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 30) & 1073741823) | ((in[ 1 + inPos] & 268435455) <<  2);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 28) & 1073741823) | ((in[ 2 + inPos] & 67108863) <<  4);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 26) & 1073741823) | ((in[ 3 + inPos] & 16777215) <<  6);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>> 24) & 1073741823) | ((in[ 4 + inPos] & 4194303) <<  8);
      out[ 5 + outPos] = ((in[ 4 + inPos] >>> 22) & 1073741823) | ((in[ 5 + inPos] & 1048575) << 10);
      out[ 6 + outPos] = ((in[ 5 + inPos] >>> 20) & 1073741823) | ((in[ 6 + inPos] & 262143) << 12);
      out[ 7 + outPos] = ((in[ 6 + inPos] >>> 18) & 1073741823) | ((in[ 7 + inPos] & 65535) << 14);
      out[ 8 + outPos] = ((in[ 7 + inPos] >>> 16) & 1073741823) | ((in[ 8 + inPos] & 16383) << 16);
      out[ 9 + outPos] = ((in[ 8 + inPos] >>> 14) & 1073741823) | ((in[ 9 + inPos] & 4095) << 18);
      out[10 + outPos] = ((in[ 9 + inPos] >>> 12) & 1073741823) | ((in[10 + inPos] & 1023) << 20);
      out[11 + outPos] = ((in[10 + inPos] >>> 10) & 1073741823) | ((in[11 + inPos] & 255) << 22);
      out[12 + outPos] = ((in[11 + inPos] >>>  8) & 1073741823) | ((in[12 + inPos] & 63) << 24);
      out[13 + outPos] = ((in[12 + inPos] >>>  6) & 1073741823) | ((in[13 + inPos] & 15) << 26);
      out[14 + outPos] = ((in[13 + inPos] >>>  4) & 1073741823) | ((in[14 + inPos] & 3) << 28);
      out[15 + outPos] = ((in[14 + inPos] >>>  2) & 1073741823);
      out[16 + outPos] = ((in[15 + inPos] >>>  0) & 1073741823);
      out[17 + outPos] = ((in[15 + inPos] >>> 30) & 1073741823) | ((in[16 + inPos] & 268435455) <<  2);
      out[18 + outPos] = ((in[16 + inPos] >>> 28) & 1073741823) | ((in[17 + inPos] & 67108863) <<  4);
      out[19 + outPos] = ((in[17 + inPos] >>> 26) & 1073741823) | ((in[18 + inPos] & 16777215) <<  6);
      out[20 + outPos] = ((in[18 + inPos] >>> 24) & 1073741823) | ((in[19 + inPos] & 4194303) <<  8);
      out[21 + outPos] = ((in[19 + inPos] >>> 22) & 1073741823) | ((in[20 + inPos] & 1048575) << 10);
      out[22 + outPos] = ((in[20 + inPos] >>> 20) & 1073741823) | ((in[21 + inPos] & 262143) << 12);
      out[23 + outPos] = ((in[21 + inPos] >>> 18) & 1073741823) | ((in[22 + inPos] & 65535) << 14);
      out[24 + outPos] = ((in[22 + inPos] >>> 16) & 1073741823) | ((in[23 + inPos] & 16383) << 16);
      out[25 + outPos] = ((in[23 + inPos] >>> 14) & 1073741823) | ((in[24 + inPos] & 4095) << 18);
      out[26 + outPos] = ((in[24 + inPos] >>> 12) & 1073741823) | ((in[25 + inPos] & 1023) << 20);
      out[27 + outPos] = ((in[25 + inPos] >>> 10) & 1073741823) | ((in[26 + inPos] & 255) << 22);
      out[28 + outPos] = ((in[26 + inPos] >>>  8) & 1073741823) | ((in[27 + inPos] & 63) << 24);
      out[29 + outPos] = ((in[27 + inPos] >>>  6) & 1073741823) | ((in[28 + inPos] & 15) << 26);
      out[30 + outPos] = ((in[28 + inPos] >>>  4) & 1073741823) | ((in[29 + inPos] & 3) << 28);
      out[31 + outPos] = ((in[29 + inPos] >>>  2) & 1073741823);
    }
  }

  private static final class Packer31 extends IntPacker {

    private Packer31() {
      super(31);
    }

    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] =
          ((in[ 0 + inPos] & 2147483647) <<   0)
        | ((in[ 1 + inPos] & 2147483647) <<  31);
      out[ 1 + outPos] =
          ((in[ 1 + inPos] & 2147483647) >>>  1)
        | ((in[ 2 + inPos] & 2147483647) <<  30);
      out[ 2 + outPos] =
          ((in[ 2 + inPos] & 2147483647) >>>  2)
        | ((in[ 3 + inPos] & 2147483647) <<  29);
      out[ 3 + outPos] =
          ((in[ 3 + inPos] & 2147483647) >>>  3)
        | ((in[ 4 + inPos] & 2147483647) <<  28);
      out[ 4 + outPos] =
          ((in[ 4 + inPos] & 2147483647) >>>  4)
        | ((in[ 5 + inPos] & 2147483647) <<  27);
      out[ 5 + outPos] =
          ((in[ 5 + inPos] & 2147483647) >>>  5)
        | ((in[ 6 + inPos] & 2147483647) <<  26);
      out[ 6 + outPos] =
          ((in[ 6 + inPos] & 2147483647) >>>  6)
        | ((in[ 7 + inPos] & 2147483647) <<  25);
      out[ 7 + outPos] =
          ((in[ 7 + inPos] & 2147483647) >>>  7)
        | ((in[ 8 + inPos] & 2147483647) <<  24);
      out[ 8 + outPos] =
          ((in[ 8 + inPos] & 2147483647) >>>  8)
        | ((in[ 9 + inPos] & 2147483647) <<  23);
      out[ 9 + outPos] =
          ((in[ 9 + inPos] & 2147483647) >>>  9)
        | ((in[10 + inPos] & 2147483647) <<  22);
      out[10 + outPos] =
          ((in[10 + inPos] & 2147483647) >>> 10)
        | ((in[11 + inPos] & 2147483647) <<  21);
      out[11 + outPos] =
          ((in[11 + inPos] & 2147483647) >>> 11)
        | ((in[12 + inPos] & 2147483647) <<  20);
      out[12 + outPos] =
          ((in[12 + inPos] & 2147483647) >>> 12)
        | ((in[13 + inPos] & 2147483647) <<  19);
      out[13 + outPos] =
          ((in[13 + inPos] & 2147483647) >>> 13)
        | ((in[14 + inPos] & 2147483647) <<  18);
      out[14 + outPos] =
          ((in[14 + inPos] & 2147483647) >>> 14)
        | ((in[15 + inPos] & 2147483647) <<  17);
      out[15 + outPos] =
          ((in[15 + inPos] & 2147483647) >>> 15)
        | ((in[16 + inPos] & 2147483647) <<  16);
      out[16 + outPos] =
          ((in[16 + inPos] & 2147483647) >>> 16)
        | ((in[17 + inPos] & 2147483647) <<  15);
      out[17 + outPos] =
          ((in[17 + inPos] & 2147483647) >>> 17)
        | ((in[18 + inPos] & 2147483647) <<  14);
      out[18 + outPos] =
          ((in[18 + inPos] & 2147483647) >>> 18)
        | ((in[19 + inPos] & 2147483647) <<  13);
      out[19 + outPos] =
          ((in[19 + inPos] & 2147483647) >>> 19)
        | ((in[20 + inPos] & 2147483647) <<  12);
      out[20 + outPos] =
          ((in[20 + inPos] & 2147483647) >>> 20)
        | ((in[21 + inPos] & 2147483647) <<  11);
      out[21 + outPos] =
          ((in[21 + inPos] & 2147483647) >>> 21)
        | ((in[22 + inPos] & 2147483647) <<  10);
      out[22 + outPos] =
          ((in[22 + inPos] & 2147483647) >>> 22)
        | ((in[23 + inPos] & 2147483647) <<   9);
      out[23 + outPos] =
          ((in[23 + inPos] & 2147483647) >>> 23)
        | ((in[24 + inPos] & 2147483647) <<   8);
      out[24 + outPos] =
          ((in[24 + inPos] & 2147483647) >>> 24)
        | ((in[25 + inPos] & 2147483647) <<   7);
      out[25 + outPos] =
          ((in[25 + inPos] & 2147483647) >>> 25)
        | ((in[26 + inPos] & 2147483647) <<   6);
      out[26 + outPos] =
          ((in[26 + inPos] & 2147483647) >>> 26)
        | ((in[27 + inPos] & 2147483647) <<   5);
      out[27 + outPos] =
          ((in[27 + inPos] & 2147483647) >>> 27)
        | ((in[28 + inPos] & 2147483647) <<   4);
      out[28 + outPos] =
          ((in[28 + inPos] & 2147483647) >>> 28)
        | ((in[29 + inPos] & 2147483647) <<   3);
      out[29 + outPos] =
          ((in[29 + inPos] & 2147483647) >>> 29)
        | ((in[30 + inPos] & 2147483647) <<   2);
      out[30 + outPos] =
          ((in[30 + inPos] & 2147483647) >>> 30)
        | ((in[31 + inPos] & 2147483647) <<   1);
    }
    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){
      out[ 0 + outPos] = ((in[ 0 + inPos] >>>  0) & 2147483647);
      out[ 1 + outPos] = ((in[ 0 + inPos] >>> 31) & 2147483647) | ((in[ 1 + inPos] & 1073741823) <<  1);
      out[ 2 + outPos] = ((in[ 1 + inPos] >>> 30) & 2147483647) | ((in[ 2 + inPos] & 536870911) <<  2);
      out[ 3 + outPos] = ((in[ 2 + inPos] >>> 29) & 2147483647) | ((in[ 3 + inPos] & 268435455) <<  3);
      out[ 4 + outPos] = ((in[ 3 + inPos] >>> 28) & 2147483647) | ((in[ 4 + inPos] & 134217727) <<  4);
      out[ 5 + outPos] = ((in[ 4 + inPos] >>> 27) & 2147483647) | ((in[ 5 + inPos] & 67108863) <<  5);
      out[ 6 + outPos] = ((in[ 5 + inPos] >>> 26) & 2147483647) | ((in[ 6 + inPos] & 33554431) <<  6);
      out[ 7 + outPos] = ((in[ 6 + inPos] >>> 25) & 2147483647) | ((in[ 7 + inPos] & 16777215) <<  7);
      out[ 8 + outPos] = ((in[ 7 + inPos] >>> 24) & 2147483647) | ((in[ 8 + inPos] & 8388607) <<  8);
      out[ 9 + outPos] = ((in[ 8 + inPos] >>> 23) & 2147483647) | ((in[ 9 + inPos] & 4194303) <<  9);
      out[10 + outPos] = ((in[ 9 + inPos] >>> 22) & 2147483647) | ((in[10 + inPos] & 2097151) << 10);
      out[11 + outPos] = ((in[10 + inPos] >>> 21) & 2147483647) | ((in[11 + inPos] & 1048575) << 11);
      out[12 + outPos] = ((in[11 + inPos] >>> 20) & 2147483647) | ((in[12 + inPos] & 524287) << 12);
      out[13 + outPos] = ((in[12 + inPos] >>> 19) & 2147483647) | ((in[13 + inPos] & 262143) << 13);
      out[14 + outPos] = ((in[13 + inPos] >>> 18) & 2147483647) | ((in[14 + inPos] & 131071) << 14);
      out[15 + outPos] = ((in[14 + inPos] >>> 17) & 2147483647) | ((in[15 + inPos] & 65535) << 15);
      out[16 + outPos] = ((in[15 + inPos] >>> 16) & 2147483647) | ((in[16 + inPos] & 32767) << 16);
      out[17 + outPos] = ((in[16 + inPos] >>> 15) & 2147483647) | ((in[17 + inPos] & 16383) << 17);
      out[18 + outPos] = ((in[17 + inPos] >>> 14) & 2147483647) | ((in[18 + inPos] & 8191) << 18);
      out[19 + outPos] = ((in[18 + inPos] >>> 13) & 2147483647) | ((in[19 + inPos] & 4095) << 19);
      out[20 + outPos] = ((in[19 + inPos] >>> 12) & 2147483647) | ((in[20 + inPos] & 2047) << 20);
      out[21 + outPos] = ((in[20 + inPos] >>> 11) & 2147483647) | ((in[21 + inPos] & 1023) << 21);
      out[22 + outPos] = ((in[21 + inPos] >>> 10) & 2147483647) | ((in[22 + inPos] & 511) << 22);
      out[23 + outPos] = ((in[22 + inPos] >>>  9) & 2147483647) | ((in[23 + inPos] & 255) << 23);
      out[24 + outPos] = ((in[23 + inPos] >>>  8) & 2147483647) | ((in[24 + inPos] & 127) << 24);
      out[25 + outPos] = ((in[24 + inPos] >>>  7) & 2147483647) | ((in[25 + inPos] & 63) << 25);
      out[26 + outPos] = ((in[25 + inPos] >>>  6) & 2147483647) | ((in[26 + inPos] & 31) << 26);
      out[27 + outPos] = ((in[26 + inPos] >>>  5) & 2147483647) | ((in[27 + inPos] & 15) << 27);
      out[28 + outPos] = ((in[27 + inPos] >>>  4) & 2147483647) | ((in[28 + inPos] & 7) << 28);
      out[29 + outPos] = ((in[28 + inPos] >>>  3) & 2147483647) | ((in[29 + inPos] & 3) << 29);
      out[30 + outPos] = ((in[29 + inPos] >>>  2) & 2147483647) | ((in[30 + inPos] & 1) << 30);
      out[31 + outPos] = ((in[30 + inPos] >>>  1) & 2147483647);
    }
  }

}
