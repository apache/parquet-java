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
package parquet.encoding.bitpacking;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Scheme designed by D. Lemire
 *
 * This is a re-implementation of The scheme released under Apache License Version 2.0
 * at https://github.com/lemire/JavaFastPFOR/blob/master/src/integercompression/BitPacking.java
 *
 * It generate two classes:
 * - LemireBitPackingLE, the original scheme, filling the LSB first
 * - LemireBitPackingBE, the scheme modified to fill the MSB first (and match our existing bit packing)
 *
 * The result of the generation is checked in. To regenerate the code run this class and check in the result.
 *
 * The generated classes pack the values into arrays of ints (as opposed to arrays of bytes) based on a given bit width.
 *
 * Note: This is not really used for now as the hadoop API does not really let write int[]. We need to revisit this
 *
 * @author Julien Le Dem
 *
 */
public class IntBasedBitPackingGenerator {

  private static final String CLASS_NAME_PREFIX = "LemireBitPacking";

  public static void main(String[] args) throws Exception {
    String basePath = args[0];
    generateScheme(CLASS_NAME_PREFIX + "BE", true, basePath);
    generateScheme(CLASS_NAME_PREFIX + "LE", false, basePath);
  }

  private static void generateScheme(String className, boolean msbFirst, String basePath) throws IOException {
    final File file = new File(basePath + "/parquet/column/values/bitpacking/" + className + ".java").getAbsoluteFile();
    if (!file.getParentFile().exists()) {
      file.getParentFile().mkdirs();
    }
    FileWriter fw = new FileWriter(file);
    fw.append("package parquet.column.values.bitpacking;\n");
    fw.append("\n");
    fw.append("/**\n");
    fw.append(" * Based on the original implementation at at https://github.com/lemire/JavaFastPFOR/blob/master/src/integercompression/BitPacking.java\n");
    fw.append(" * Which is released under the\n");
    fw.append(" * Apache License Version 2.0 http://www.apache.org/licenses/.\n");
    fw.append(" * By Daniel Lemire, http://lemire.me/en/\n");
    fw.append(" * \n");
    fw.append(" * Scheme designed by D. Lemire\n");
    if (msbFirst) {
      fw.append(" * Adapted to pack from the Most Significant Bit first\n");
    }
    fw.append(" * \n");
    fw.append(" * @author automatically generated\n");
    fw.append(" * @see IntBasedBitPackingGenerator\n");
    fw.append(" *\n");
    fw.append(" */\n");
    fw.append("abstract class " + className + " {\n");
    fw.append("\n");
    fw.append("  private static final IntPacker[] packers = new IntPacker[32];\n");
    fw.append("  static {\n");
    for (int i = 0; i < 32; i++) {
      fw.append("    packers[" + i + "] = new Packer" + i + "();\n");
    }
    fw.append("  }\n");
    fw.append("\n");
    fw.append("  public static final IntPackerFactory factory = new IntPackerFactory() {\n");
    fw.append("    public IntPacker newIntPacker(int bitWidth) {\n");
    fw.append("      return packers[bitWidth];\n");
    fw.append("    }\n");
    fw.append("  };\n");
    fw.append("\n");
    for (int i = 0; i < 32; i++) {
      generateClass(fw, i, msbFirst);
      fw.append("\n");
    }
    fw.append("}\n");
    fw.close();
  }

  private static void generateClass(FileWriter fw, int bitWidth, boolean msbFirst) throws IOException {
    int mask = 0;
    for (int i = 0; i < bitWidth; i++) {
      mask <<= 1;
      mask |= 1;
    }
    fw.append("  private static final class Packer" + bitWidth + " extends IntPacker {\n");
    fw.append("\n");
    fw.append("    private Packer" + bitWidth + "() {\n");
    fw.append("      super(" + bitWidth + ");\n");
    fw.append("    }\n");
    fw.append("\n");
    // Packing
    fw.append("    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos) {\n");
    for (int i = 0; i < bitWidth; ++i) {
      fw.append("      out[" + align(i, 2) + " + outPos] =\n");
      int startIndex = (i * 32) / bitWidth;
      int endIndex = ((i + 1) * 32 + bitWidth - 1) / bitWidth;
      for (int j = startIndex; j < endIndex; j++) {
        if (j == startIndex) {
          fw.append("          ");
        } else {
          fw.append("\n        | ");
        }
        String shiftString = getPackShiftString(bitWidth, i, startIndex, j, msbFirst);
        fw.append("((in[" + align(j, 2) + " + inPos] & " + mask + ")" + shiftString + ")");
      }
      fw.append(";\n");
    }
    fw.append("    }\n");

    // Unpacking
    fw.append("    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos) {\n");
    if (bitWidth > 0) {
      for (int i = 0; i < 32; ++i) {
        fw.append("      out[" + align(i, 2) + " + outPos] =");
        int byteIndex = i * bitWidth / 32;
        String shiftString = getUnpackShiftString(bitWidth, i, msbFirst);
        fw.append(" ((in[" + align(byteIndex, 2) + " + inPos] " + shiftString + ") & " + mask + ")");
        if (((i + 1) * bitWidth - 1 ) / 32 != byteIndex) {
          // reading the end of the value from next int
          int bitsRead = ((i + 1) * bitWidth - 1) % 32 + 1;
          fw.append(" | ((in[" + align(byteIndex + 1, 2) + " + inPos]");
          if (msbFirst) {
            fw.append(") >>> " + align(32 - bitsRead, 2) + ")");
          } else {
            int lowerMask = 0;
            for (int j = 0; j < bitsRead; j++) {
              lowerMask <<= 1;
              lowerMask |= 1;
            }
            fw.append(" & " + lowerMask + ") << " + align(bitWidth - bitsRead, 2) + ")");
          }
        }
        fw.append(";\n");
      }
    }
    fw.append("    }\n");
    fw.append("  }\n");
  }

  private static String getUnpackShiftString(int bitWidth, int i, boolean msbFirst) {
    final int regularShift = i * bitWidth % 32;
    String shiftString;
    if (msbFirst) {
      int shift = 32 - (regularShift + bitWidth);
      if (shift < 0) {
        shiftString = "<<  " + align(-shift, 2);
      } else {
        shiftString = ">>> " + align(shift, 2);
      }
    } else {
      shiftString = ">>> " + align(regularShift, 2);
    }
    return shiftString;
  }

  private static String getPackShiftString(int bitWidth, int integerIndex, int startIndex, int valueIndex, boolean msbFirst) {
    String shiftString;
    int regularShift = (valueIndex * bitWidth) % 32;
    if (msbFirst) { // filling most significant bit first
      int shift = 32 - (regularShift + bitWidth);
      if (valueIndex == startIndex && (integerIndex * 32) % bitWidth != 0) {
        // end of last value from previous int
          shiftString = " <<  " + align(32 - (((valueIndex + 1) * bitWidth) % 32), 2);
      } else if (shift < 0) {
        // partial last value
          shiftString = " >>> " + align(-shift, 2);
      } else {
        shiftString = " <<  " + align(shift, 2);
      }
    } else { // filling least significant bit first
      if (valueIndex == startIndex && (integerIndex * 32) % bitWidth != 0) {
        // end of last value from previous int
        shiftString = " >>> " + align(32 - regularShift, 2);
      } else {
        shiftString = " <<  " + align(regularShift, 2);
      }
    }
    return shiftString;
  }

  private static String align(int value, int digits) {
    final String valueString = String.valueOf(value);
    StringBuilder result = new StringBuilder();
    for (int i = valueString.length(); i < digits; i++) {
      result.append(" ");
    }
    result.append(valueString);
    return result.toString();
  }
}
