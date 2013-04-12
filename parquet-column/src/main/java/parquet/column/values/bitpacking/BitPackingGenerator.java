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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Scheme designed by D. Lemire
 *
 * This is a re-implementation of The scheme released under Apache License Version 2.0
 * at https://github.com/lemire/JavaFastPFOR/blob/master/src/integercompression/BitPacking.java
 * The order of values is modified to match our existing implementation.
 *
 * @author Julien Le Dem
 *
 */
public class BitPackingGenerator {

  private static final String className = "LemireBitPacking";

  public static void main(String[] args) throws Exception {
    final File file = new File("src/main/java/parquet/column/values/bitpacking/" + className + ".java");
    FileWriter fw = new FileWriter(file);
    fw.append("/**\n");
    fw.append(" * This is code is released under the\n");
    fw.append(" * Apache License Version 2.0 http://www.apache.org/licenses/.\n");
    fw.append(" *\n");
    fw.append(" * (c) Daniel Lemire, http://lemire.me/en/\n");
    fw.append(" */\n");
    fw.append("package parquet.column.values.bitpacking;\n");
    fw.append("\n");
    fw.append("/**\n");
    fw.append(" * Scheme designed by D. Lemire\n");
    fw.append(" * \n");
    fw.append(" * @author automatically generated\n");
    fw.append(" * @see BitPackingGenerator\n");
    fw.append(" *\n");
    fw.append(" */\n");
    fw.append("public abstract class LemireBitPacking {\n");
    fw.append("\n");
    fw.append("  private static final " + className + "[] packers = new " + className + "[32];\n");
    fw.append("  static {\n");
    for (int i = 0; i < 32; i++) {
      fw.append("    packers[" + i + "] = new Packer" + i + "();\n");
    }
    fw.append("  }\n");
    fw.append("\n");
    fw.append("  public static final " + className + " getPacker(int bitWidth) {\n");
    fw.append("    return packers[bitWidth];\n");
    fw.append("  }\n");
    fw.append("\n");
    fw.append("public abstract void pack32Values(int[] in, int inPos, int[] out, int outPos);\n");
    fw.append("\n");
    fw.append("public abstract void unpack32Values(int[] in, int inPos, int[] out, int outPos);\n");
    fw.append("\n");
    for (int i = 0; i < 32; i++) {
      generateClass(fw, i);
      fw.append("\n");
    }
    fw.append("}\n");
    fw.close();
  }

  private static void generateClass(FileWriter fw, int bitWidth) throws IOException {
    int mask = 0;
    for (int i = 0; i < bitWidth; i++) {
      mask <<= 1;
      mask |= 1;
    }
    fw.append("  private static final class Packer" + bitWidth + " extends " + className + " {\n");

    // Packing
    fw.append("    public final void pack32Values(final int[] in, final int inPos, final int[] out, final int outPos){\n");
    for (int i = 0; i < bitWidth; ++i) {
      fw.append("      out[" + align(i, 2) + " + outPos] =\n");
      int startIndex = (i * 32) / bitWidth;
      int endIndex = ((i + 1) * 32 + bitWidth - 1) / bitWidth;
      for (int j = startIndex; j < endIndex; j++) {
        String shiftString;
        int regularShift = (j * bitWidth) % 32;
        int shift = 32 - (regularShift + bitWidth);
        if (j == startIndex) {
          fw.append("          ");
          if ((i * 32) % bitWidth != 0) {
            shiftString = " <<  " + align(32 - (((j + 1) * bitWidth) % 32), 2); // end of last value from previous int
          } else {
            shiftString = " <<  " + align(shift, 2);
          }
        } else {
          fw.append("\n        | ");
          if (shift < 0) {
            shiftString = " >>> " + align(-shift, 2);
          } else {
            shiftString = " <<  " + align(shift, 2);
          }
        }
        fw.append("((in[" + align(j, 2) + " + inPos] & " + mask + ")" + shiftString + ")");
      }
      fw.append(";\n");
    }
    fw.append("    }\n");

    // Unpacking
    fw.append("    public final void unpack32Values(final int[] in, final int inPos, final int[] out, final int outPos){\n");
    if (bitWidth > 0) {
      for (int i = 0; i < 32; ++i) {
        fw.append("      out[" + align(i, 2) + " + outPos] =");
        int byteIndex = i * bitWidth / 32;
        final int regularShift = i * bitWidth % 32;
        int shift = 32 - (regularShift + bitWidth);
        String shiftString;
        if (shift < 0) {
          shiftString = "<<  " + align(-shift, 2);
        } else {
          shiftString = ">>> " + align(shift, 2);
        }
        fw.append(" ((in[" + align(byteIndex, 2) + " + inPos] " + shiftString + ") & " + mask + ")");
        if (((i + 1) * bitWidth - 1 ) / 32 != byteIndex) {
          int bitsRead = ((i + 1) * bitWidth - 1) % 32 + 1;
          fw.append(" | ((in[" + align(byteIndex + 1, 2) + " + inPos]) >>> " + align(32 - bitsRead, 2) + ")");
        }
        fw.append(";\n");
      }
    }
    fw.append("    }\n");
    fw.append("  }\n");
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
