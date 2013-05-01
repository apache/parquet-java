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

import parquet.bytes.BytesUtils;

/**
 *
 * This class generates bit packers that pack the most significant bit first.
 * The result of the generation is checked in. To regenerate the code run this class and check in the result.
 *
 * TODO: remove the unnecessary masks for perf
 *
 * @author Julien Le Dem
 *
 */
public class ByteBasedBitPackingGenerator {

  private static final String className = "ByteBitPacking";

  public static void main(String[] args) throws Exception {
    final File file = new File("src/main/java/parquet/column/values/bitpacking/" + className + ".java");
    FileWriter fw = new FileWriter(file);
    fw.append("package parquet.column.values.bitpacking;\n");
    fw.append("\n");
    fw.append("/**\n");
    fw.append(" * Packs from the Most Significant Bit first\n");
    fw.append(" * \n");
    fw.append(" * @author automatically generated\n");
    fw.append(" * @see ByteBasedBitPackingGenerator\n");
    fw.append(" *\n");
    fw.append(" */\n");
    fw.append("public abstract class " + className + " {\n");
    fw.append("\n");
    fw.append("  private static final BytePacker[] packers = new BytePacker[33];\n");
    fw.append("  static {\n");
    for (int i = 0; i <= 32; i++) {
      fw.append("    packers[" + i + "] = new Packer" + i + "();\n");
    }
    fw.append("  }\n");
    fw.append("\n");
    fw.append("  public static final BytePacker getPacker(int bitWidth) {\n");
    fw.append("    return packers[bitWidth];\n");
    fw.append("  }\n");
    fw.append("\n");
    for (int i = 0; i <= 32; i++) {
      generateClass(fw, i);
      fw.append("\n");
    }
    fw.append("}\n");
    fw.close();
  }

  private static void generateClass(FileWriter fw, int bitWidth) throws IOException {
    fw.append("  private static final class Packer" + bitWidth + " extends BytePacker {\n");
    fw.append("\n");
    fw.append("    private Packer" + bitWidth + "() {\n");
    fw.append("      super("+bitWidth+");\n");
    fw.append("    }\n");
    fw.append("\n");
    // Packing
    generatePack(fw, bitWidth, 1);
    generatePack(fw, bitWidth, 4);

    // Unpacking
    generateUnpack(fw, bitWidth, 1);
    generateUnpack(fw, bitWidth, 4);

    fw.append("  }\n");
  }

  private static void generatePack(FileWriter fw, int bitWidth, int batch) throws IOException {
    int mask = genMask(bitWidth);
    fw.append("    public final void pack" + (batch * 8) + "Values(final int[] in, final int inPos, final byte[] out, final int outPos) {\n");
    for (int byteIndex = 0; byteIndex < bitWidth * batch; ++byteIndex) {
      fw.append("      out[" + align(byteIndex, 2) + " + outPos] = (byte)((\n");
      int startIndex = (byteIndex * 8) / bitWidth;
      int endIndex = ((byteIndex + 1) * 8 + bitWidth - 1) / bitWidth;
      for (int valueIndex = startIndex; valueIndex < endIndex; valueIndex++) {
        if (valueIndex == startIndex) {
          fw.append("          ");
        } else {
          fw.append("\n        | ");
        }
        // relative positions of the start and end of the value to the start and end of the byte
        int valueStartBitIndex = (valueIndex * bitWidth) - (8 * (byteIndex));
        int valueEndBitIndex = ((valueIndex + 1) * bitWidth) - (8 * (byteIndex + 1));

        // boundaries of the current value that we want
        int valueStartBitWanted = valueStartBitIndex < 0 ? bitWidth - 1 + valueStartBitIndex : bitWidth - 1;
        int valueEndBitWanted = valueEndBitIndex > 0 ? valueEndBitIndex : 0;

        // boundaries of the current byte that will receive them
        int byteStartBitWanted = valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex;
        int byteEndBitWanted = valueEndBitIndex > 0 ? 0 : -valueEndBitIndex;
        visualizeAlignment(
            fw, bitWidth, valueEndBitIndex,
            valueStartBitWanted, valueEndBitWanted,
            byteStartBitWanted, byteEndBitWanted
            );
        int shift = valueEndBitWanted - byteEndBitWanted;
        String shiftString = ""; // used when shift == 0
        if (shift > 0) {
          shiftString = " >>> " + shift;
        } else if (shift < 0) {
          shiftString = " <<  " + ( - shift);
        }
        fw.append("((in[" + align(valueIndex, 2) + " + inPos] & " + mask + ")" + shiftString + ")");
      }
      fw.append(") & 255);\n");
    }
    fw.append("    }\n");
  }

  private static void visualizeAlignment(FileWriter fw, int bitWidth,
      int valueEndBitIndex, int valueStartBitWanted, int valueEndBitWanted,
      int byteStartBitWanted, int byteEndBitWanted) throws IOException {
    // ASCII art to visualize what is happening
    fw.append("//");
    int buf = 2 + Math.max(0, bitWidth + 8);
    for (int i = 0; i < buf; i++) {
      fw.append(" ");
    }
    fw.append("[");
    for (int i = 7; i >= 0; i--) {
      if (i<=byteStartBitWanted && i>=byteEndBitWanted) {
        fw.append(String.valueOf(i));
      } else {
        fw.append("_");
      }
    }
    fw.append("]\n          //");
    for (int i = 0; i < buf + 8 - bitWidth + valueEndBitIndex ; i++) {
      fw.append(" ");
    }
    fw.append("[");
    for (int i = bitWidth - 1; i >= 0 ; i--) {
      if (i<=valueStartBitWanted && i>=valueEndBitWanted) {
        fw.append(String.valueOf(i % 10));
      } else {
        fw.append("_");
      }
    }
    fw.append("]\n");
    fw.append("           ");
  }

  private static void generateUnpack(FileWriter fw, int bitWidth, int batch)
      throws IOException {
    fw.append("    public final void unpack" + (batch * 8) + "Values(final byte[] in, final int inPos, final int[] out, final int outPos) {\n");
    if (bitWidth > 0) {
      int mask = genMask(bitWidth);
      for (int valueIndex = 0; valueIndex < (batch * 8); ++valueIndex) {
        fw.append("      out[" + align(valueIndex, 2) + " + outPos] =\n");

        int startIndex = valueIndex * bitWidth / 8;
        int endIndex = BytesUtils.paddedByteCountFromBits((valueIndex + 1) * bitWidth);

        for (int byteIndex = startIndex; byteIndex < endIndex; byteIndex++) {
          if (byteIndex == startIndex) {
            fw.append("          ");
          } else {
            fw.append("\n        | ");
          }
          // relative positions of the start and end of the value to the start and end of the byte
          int valueStartBitIndex = (valueIndex * bitWidth) - (8 * (byteIndex));
          int valueEndBitIndex = ((valueIndex + 1) * bitWidth) - (8 * (byteIndex + 1));

          // boundaries of the current value that we want
          int valueStartBitWanted = valueStartBitIndex < 0 ? bitWidth - 1 + valueStartBitIndex : bitWidth - 1;
          int valueEndBitWanted = valueEndBitIndex > 0 ? valueEndBitIndex : 0;

          // boundaries of the current byte that will receive them
          int byteStartBitWanted = valueStartBitIndex < 0 ? 8 : 7 - valueStartBitIndex;
          int byteEndBitWanted = valueEndBitIndex > 0 ? 0 : -valueEndBitIndex;
          visualizeAlignment(
              fw, bitWidth, valueEndBitIndex,
              valueStartBitWanted, valueEndBitWanted,
              byteStartBitWanted, byteEndBitWanted
              );

          int shift = valueEndBitIndex;
          String shiftString = ""; // when shift == 0
          if (shift < 0) {
            shiftString = ">>>  " + (-shift);
          } else if (shift > 0){
            shiftString = "<<  " + shift;
          }
          fw.append(" (((((int)in[" + align(byteIndex, 2) + " + inPos]) & 255) " + shiftString + ") & " + mask + ")");
        }
        fw.append(";\n");
      }
    }
    fw.append("    }\n");
  }

  private static int genMask(int bitWidth) {
    int mask = 0;
    for (int i = 0; i < bitWidth; i++) {
      mask <<= 1;
      mask |= 1;
    }
    return mask;
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
