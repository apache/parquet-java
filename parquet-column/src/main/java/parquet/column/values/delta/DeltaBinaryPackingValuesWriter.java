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

  public static final int MAX_BITWIDTH = 32;
  private final int miniBlockSizeInValues;
  private final int blockSizeInValues;
  private final int miniBlockNum;
  private final CapacityByteArrayOutputStream baos;
  private int totalValueCount = 0;
  private int valueToFlush = 0;
  private int[] deltaBlockBuffer;
  /*bytes buffer for a mini block, it is reused for each mini block. therefore the size of biggest miniblock with bitwith of 32 is allocated*/
  private byte[] miniBlockByteBuffer;
  private int firstValue = 0;
  private int previousValue = 0;
  private int minDeltaInCurrentBlock = Integer.MAX_VALUE;

  public DeltaBinaryPackingValuesWriter(int blockSizeInValues, int miniBlockNum, int slabSize) {
    this.blockSizeInValues = blockSizeInValues;
    this.miniBlockNum = miniBlockNum;
    this.miniBlockSizeInValues = blockSizeInValues / miniBlockNum;
    assert (miniBlockSizeInValues % 8 == 0) : "miniBlockSize must be multiple of 8";
    deltaBlockBuffer = new int[blockSizeInValues];
    miniBlockByteBuffer = new byte[miniBlockSizeInValues * MAX_BITWIDTH];
    baos = new CapacityByteArrayOutputStream(slabSize);
  }

  @Override
  public long getBufferedSize() {
    return 0;
  }

  @Override
  public void writeInteger(int v) {
    valueToFlush++;
    if (totalValueCount == 0) {
      firstValue = v;
      previousValue = firstValue;
    }


    int delta = v - previousValue;//calculate delta
    previousValue = v;

    deltaBlockBuffer[(totalValueCount++) % blockSizeInValues] = delta;


    if (delta < minDeltaInCurrentBlock)
      minDeltaInCurrentBlock = delta;

    if (blockSizeInValues == valueToFlush)
      flushWholeBlockBuffer();
  }

  private void flushWholeBlockBuffer() {
    //this method may flush the whole buffer or only part of the buffer
    int[] bitWiths = new int[miniBlockNum];

    int countToFlush = getCountToFlush();
    int miniBlocksToFlush = getMiniBlockToFlush(countToFlush);

    //since we store the min delta, the deltas will be converted to be delta of deltas and will always be positive or 0
    for (int i = 0; i < countToFlush; i++) {
      deltaBlockBuffer[i] = deltaBlockBuffer[i] - minDeltaInCurrentBlock;
    }

    try {
      BytesUtils.writeUnsignedVarInt(minDeltaInCurrentBlock, baos);
    } catch (IOException e) {
      throw new ParquetEncodingException("can not write min delta for block", e);
    }

    calculateBitWithsForBlockBuffer(bitWiths);
    for (int i = 0; i < miniBlockNum; i++) {
      try {
        BytesUtils.writeIntLittleEndianOnOneByte(baos, bitWiths[i]);
      } catch (IOException e) {
        throw new ParquetEncodingException("can not write bitwith for miniblock");
      }
    }//first m bytes are for bitwiths...header of miniblock


    for (int i = 0; i < miniBlocksToFlush; i++) {
      //writing i th miniblock
      int currentBitWidth = bitWiths[i];
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(currentBitWidth);
      //allocate output bytes TODO, this can be reused...
      int miniBlockStart = i * miniBlockSizeInValues;
      for (int j = miniBlockStart; j < (i + 1) * miniBlockSizeInValues; j += 8) {//8 values per pack
        //This might write more values, since it's not aligend to miniblock, but doesnt matter. The reader uses total count to see if reached the end. And mini block is atomic in terms of flushing
        packer.pack8Values(deltaBlockBuffer, j, miniBlockByteBuffer, 0); //TODO: bug!! flush should be on mini block basis,
        baos.write(miniBlockByteBuffer, 0, currentBitWidth);
      }

    }

    minDeltaInCurrentBlock = Integer.MAX_VALUE;
    valueToFlush = 0;
  }

  private void calculateBitWithsForBlockBuffer(int[] bitWiths) {
    int numberCount = getCountToFlush();

    int miniBlocksToFlush = getMiniBlockToFlush(numberCount);

    for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksToFlush; miniBlockIndex++) {
      //iterate through values in each mini block
      int mask = 0;
      int miniStart = miniBlockIndex * miniBlockSizeInValues;
      int miniEnd = Math.min((miniBlockIndex + 1) * miniBlockSizeInValues, numberCount);
      for (int valueIndex = miniStart; valueIndex < miniEnd; valueIndex++) {
        mask |= deltaBlockBuffer[valueIndex];
      }
      bitWiths[miniBlockIndex] = 32 - Integer.numberOfLeadingZeros(mask);
    }
  }

  private int getMiniBlockToFlush(double numberCount) {
    return (int) Math.ceil(numberCount / miniBlockSizeInValues);
  }

  private int getCountToFlush() {
    int numberCount = totalValueCount % blockSizeInValues;
    if (numberCount == 0)
      numberCount = blockSizeInValues;
    return numberCount;
  }

  @Override
  public BytesInput getBytes() {
    //The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
    if (valueToFlush != 0)
      flushWholeBlockBuffer();//TODO: bug, when getBytes is called multiple times
    return BytesInput.concat(
            BytesInput.fromUnsignedVarInt(blockSizeInValues),
            BytesInput.fromUnsignedVarInt(miniBlockNum),
            BytesInput.fromUnsignedVarInt(totalValueCount),
            BytesInput.fromUnsignedVarInt(firstValue),
            BytesInput.from(baos));
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.DELTA_BINARY_PACKED;
  }

  @Override
  public void reset() {
    this.totalValueCount = 0;
    this.baos.reset();
    this.valueToFlush = 0;
    this.minDeltaInCurrentBlock = Integer.MAX_VALUE;
  }

  @Override
  public long getAllocatedSize() {
    return baos.getCapacity();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s DeltaBinaryPacking %d bytes", prefix, getAllocatedSize());
  }
}
