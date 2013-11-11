package parquet.column.values.delta;


import parquet.bytes.BytesInput;

class DeltaBinaryPackingConfig {
   final int blockSizeInValues;
   final int miniBlockNum;
   final int miniBlockSizeInValues;

  public DeltaBinaryPackingConfig(int blockSizeInValues, int miniBlockNum) {
    this.blockSizeInValues=blockSizeInValues;
    this.miniBlockNum=miniBlockNum;
    this.miniBlockSizeInValues = blockSizeInValues / miniBlockNum;
    assert (miniBlockSizeInValues % 8 == 0) : "miniBlockSize must be multiple of 8";
    //TODO: change to use Precondition
  }

  public BytesInput toBytesInput(){
    return BytesInput.concat(
            BytesInput.fromUnsignedVarInt(blockSizeInValues),
            BytesInput.fromUnsignedVarInt(miniBlockNum));
  }
}
