package parquet.column.values.bitpacking;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Scheme designed by D. Lemire
 *
 * @author Julien Le Dem
 *
 */
public class BitPackingGenerator {

  private static final String className = "LemireBitPacking";

  public static void main(String[] args) throws Exception {
    final File file = new File("src/main/java/parquet/column/values/bitpacking/" + className + ".java");
    FileWriter fw = new FileWriter(file);
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
        String shift;
        if (j == startIndex) {
          fw.append("          ");
          if ((i * 32) % bitWidth != 0) {
            shift = " >>> " + align(32 - (j * bitWidth) % 32, 2);
          } else {
            shift = " <<  " + align((j * bitWidth) % 32, 2);
          }
        } else {
          fw.append("\n        | ");
          shift = " <<  " + align((j * bitWidth) % 32, 2);
        }
        fw.append("((in[" + align(j, 2) + " + inPos] & " + mask + ")" + shift + ")");
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
        fw.append(" ((in[" + align(byteIndex, 2) + " + inPos] >>> " + align((i * bitWidth % 32), 2) + ") & " + mask + ")");
        if (((i + 1) * bitWidth - 1 ) / 32 != byteIndex) {
          int bitsRead = ((i + 1) * bitWidth - 1) % 32 + 1;
          int lowerMask = 0;
          for (int j = 0; j < bitsRead; j++) {
            lowerMask <<= 1;
            lowerMask |= 1;
          }
          fw.append(" | ((in[" + align(byteIndex + 1, 2) + " + inPos] & " + lowerMask + ") << " + align(bitWidth - bitsRead, 2) + ")");
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
