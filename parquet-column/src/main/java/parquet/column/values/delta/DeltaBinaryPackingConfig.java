package parquet.column.values.delta;


import parquet.Preconditions;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Config for delta binary packing
 *
 * @author Tianshuo Deng
 */
class DeltaBinaryPackingConfig {
  final int blockSizeInValues;
  final int miniBlockNumInABlock;
  final int miniBlockSizeInValues;

  public DeltaBinaryPackingConfig(int blockSizeInValues, int miniBlockNumInABlock) {
    this.blockSizeInValues = blockSizeInValues;
    this.miniBlockNumInABlock = miniBlockNumInABlock;
    double miniSize = (double) blockSizeInValues / miniBlockNumInABlock;
    Preconditions.checkArgument(miniSize % 8 == 0, "miniBlockSize must be multiple of 8, but it's " + miniSize);
    this.miniBlockSizeInValues = (int) miniSize;
  }

  public static DeltaBinaryPackingConfig readConfig(InputStream in) throws IOException {
    return new DeltaBinaryPackingConfig(BytesUtils.readUnsignedVarInt(in),
            BytesUtils.readUnsignedVarInt(in));
  }

  public BytesInput toBytesInput() {
    return BytesInput.concat(
            BytesInput.fromUnsignedVarInt(blockSizeInValues),
            BytesInput.fromUnsignedVarInt(miniBlockNumInABlock));
  }
}
