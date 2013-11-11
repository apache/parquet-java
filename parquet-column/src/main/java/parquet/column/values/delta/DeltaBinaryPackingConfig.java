package parquet.column.values.delta;


import parquet.bytes.BytesInput;

class DeltaBinaryPackingConfig {
   final int blockSizeInValues;
   final int miniBlockNum;
   final int miniBlockSizeInValues;

  public DeltaBinaryPackingConfig(int blockSizeInValues, int miniBlockNum) {
    this.blockSizeInValues=blockSizeInValues;
    this.miniBlockNum=miniBlockNum;
    double miniSize = (double) blockSizeInValues / miniBlockNum;
    assert (miniSize % 8 == 0) : "miniBlockSize must be multiple of 8";
    this.miniBlockSizeInValues = (int) miniSize;
    //TODO: change to use Precondition
  }

  public BytesInput toBytesInput(){
    return BytesInput.concat(
            BytesInput.fromUnsignedVarInt(blockSizeInValues),
            BytesInput.fromUnsignedVarInt(miniBlockNum));
  }
}
