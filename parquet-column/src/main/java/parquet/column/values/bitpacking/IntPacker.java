package parquet.column.values.bitpacking;

public abstract class IntPacker {

  private final int bitWidth;

  IntPacker(int bitWidth) {
    this.bitWidth = bitWidth;
  }

  public final int getBitWidth() {
    return bitWidth;
  }

  public abstract void pack32Values(int[] in, int inPos, int[] out, int outPos);

  public abstract void unpack32Values(int[] in, int inPos, int[] out, int outPos);

}