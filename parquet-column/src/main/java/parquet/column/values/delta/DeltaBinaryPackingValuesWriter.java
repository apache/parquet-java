package parquet.column.values.delta;

import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;
import parquet.bytes.CapacityByteArrayOutputStream;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.bitpacking.BytePacker;
import parquet.column.values.bitpacking.Packer;
import parquet.io.ParquetEncodingException;

import java.io.IOException;

/**
 * Write integers with delta encoding and binary packing
 * This Writer will write the value when a group is finished(o)
 */
public class DeltaBinaryPackingValuesWriter extends ValuesWriter {

  private final int miniBlockSizeInValues;
  private final int blockSizeInValues;
  private final int miniBlockNum;
  private final CapacityByteArrayOutputStream baos;
  private int totalValueCount = 0;
  private int[] blockBuffer;

  public DeltaBinaryPackingValuesWriter(int blockSizeInValues, int miniBlockNum, int slabSize) {
    this.blockSizeInValues = blockSizeInValues;
    this.miniBlockNum = miniBlockNum;
    this.miniBlockSizeInValues = blockSizeInValues / miniBlockNum;
    blockBuffer = new int[blockSizeInValues];
    baos = new CapacityByteArrayOutputStream(slabSize);
  }

  @Override
  public long getBufferedSize() {
    return 0;
  }

  @Override
  public void writeInteger(int v) {
    blockBuffer[(totalValueCount++)%blockSizeInValues] = v;

    if (totalValueCount % blockSizeInValues == 0)
      flushWholeBlockBuffer();
  }

  private void flushWholeBlockBuffer() {
    System.out.println("flushing block");
    //TODO: caculate maxBits for each minigroup
    int[] bitWiths = new int[miniBlockNum];
    calculateBitWithsForBlockBuffer(bitWiths);
    //TODO write bitWithsintoHeader,
    for (int i = 0; i < miniBlockNum; i++) {
      try {
        BytesUtils.writeIntLittleEndianOnOneByte(baos, bitWiths[i]);
      } catch (IOException e) {
        throw new ParquetEncodingException("can not write bitwith for miniblock");
      }
    }//first m bytes are for bitwiths...header of miniblock
    //TODO: write eachMiniGroupWithBitPacking
    //TODO: number of values in each mini block must be multiple of 8, otherwise there is a bug
    for (int i = 0; i < miniBlockNum; i++) {
      //writing i th miniblock
      int currentBitWidth = bitWiths[i];
      System.out.println("bitWith is " + currentBitWidth);
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(currentBitWidth);
      //allocate output bytes TODO, this can be reused...
      byte[] output = new byte[currentBitWidth * miniBlockSizeInValues/8];
      int miniBlockStart = i * miniBlockSizeInValues;
      for (int j = miniBlockStart; j < (i + 1) * miniBlockSizeInValues; j += 8) {//8 values per pack
        int outputOffset = j - miniBlockStart;
//        System.out.println(i+" "+j+" "+localOffset);
        packer.pack8Values(blockBuffer, j, output, outputOffset*currentBitWidth/8);
      }

      try {
        baos.write(output);
      } catch (IOException e) {
        throw new ParquetEncodingException("can not write miniblock", e);
      }
    }
  }

  private void calculateBitWithsForBlockBuffer(int[] bitWiths) {
    for (int miniBlockIndex = 0; miniBlockIndex < miniBlockNum; miniBlockIndex++) {
      //iterate through values in each mini block
      int mask = 0;
      for (int valueIndex = miniBlockIndex * miniBlockSizeInValues; valueIndex < (miniBlockIndex + 1) * miniBlockSizeInValues; valueIndex++) {
        mask |= blockBuffer[valueIndex];
      }
      bitWiths[miniBlockIndex] = 32 - Integer.numberOfLeadingZeros(mask);
    }
  }

  @Override
  public BytesInput getBytes() {
    //The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
    return BytesInput.concat(
            BytesInput.fromInt(blockSizeInValues),
            BytesInput.fromInt(miniBlockNum),
            BytesInput.fromInt(totalValueCount),
            BytesInput.from(baos));
  }

  @Override
  public Encoding getEncoding() {
    return null;
  }

  @Override
  public void reset() {

  }

  @Override
  public long getAllocatedSize() {
    return 0;
  }

  @Override
  public String memUsageString(String prefix) {
    return null;
  }
}
