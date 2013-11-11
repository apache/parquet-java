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
  private final CapacityByteArrayOutputStream baos;
  private final DeltaBinaryPackingConfig config;
  private int totalValueCount = 0;
  private int deltaValuesToFlush = 0;
  private int[] deltaBlockBuffer;
  /*bytes buffer for a mini block, it is reused for each mini block. therefore the size of biggest miniblock with bitwith of 32 is allocated*/
  private byte[] miniBlockByteBuffer;
  private int firstValue = 0;
  private int previousValue = 0;
  private int minDeltaInCurrentBlock = Integer.MAX_VALUE;

  public DeltaBinaryPackingValuesWriter(int blockSizeInValues, int miniBlockNum, int slabSize) {
    this.config=new DeltaBinaryPackingConfig(blockSizeInValues,miniBlockNum);
    deltaBlockBuffer = new int[blockSizeInValues];
    miniBlockByteBuffer = new byte[config.miniBlockSizeInValues * MAX_BITWIDTH];
    baos = new CapacityByteArrayOutputStream(slabSize);
  }

  @Override
  public long getBufferedSize() {
    return 0;
  }

  @Override
  public void writeInteger(int v) {
    totalValueCount++;

    if (totalValueCount == 1) {
      firstValue = v;
      previousValue = firstValue;
      return;
    }

    int delta = v - previousValue;//calculate delta
    previousValue = v;

    deltaBlockBuffer[deltaValuesToFlush++] = delta;


    if (delta < minDeltaInCurrentBlock)
      minDeltaInCurrentBlock = delta;

    if (config.blockSizeInValues == deltaValuesToFlush)
      flushWholeBlockBuffer();
  }

  private void flushWholeBlockBuffer() {
    //this method may flush the whole buffer or only part of the buffer
    int[] bitWiths = new int[config.miniBlockNum];

    int countToFlush = deltaValuesToFlush;//TODO: unnecessary variable
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
    for (int i = 0; i < config.miniBlockNum; i++) {
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
      int miniBlockStart = i * config.miniBlockSizeInValues;
      for (int j = miniBlockStart; j < (i + 1) * config.miniBlockSizeInValues; j += 8) {//8 values per pack
        //This might write more values, since it's not aligend to miniblock, but doesnt matter. The reader uses total count to see if reached the end. And mini block is atomic in terms of flushing
        packer.pack8Values(deltaBlockBuffer, j, miniBlockByteBuffer, 0); //TODO: bug!! flush should be on mini block basis,
        baos.write(miniBlockByteBuffer, 0, currentBitWidth);
      }

    }

    minDeltaInCurrentBlock = Integer.MAX_VALUE;
    deltaValuesToFlush = 0;
  }

  private void calculateBitWithsForBlockBuffer(int[] bitWiths) {
    int numberCount = deltaValuesToFlush;//TODO: unecessary variable

    int miniBlocksToFlush = getMiniBlockToFlush(numberCount);

    for (int miniBlockIndex = 0; miniBlockIndex < miniBlocksToFlush; miniBlockIndex++) {
      //iterate through values in each mini block
      int mask = 0;
      int miniStart = miniBlockIndex * config.miniBlockSizeInValues;
      int miniEnd = Math.min((miniBlockIndex + 1) * config.miniBlockSizeInValues, numberCount);
      for (int valueIndex = miniStart; valueIndex < miniEnd; valueIndex++) {
        mask |= deltaBlockBuffer[valueIndex];
      }
      bitWiths[miniBlockIndex] = 32 - Integer.numberOfLeadingZeros(mask);
    }
  }

  private int getMiniBlockToFlush(double numberCount) {
    return (int) Math.ceil(numberCount / config.miniBlockSizeInValues);
  }



  @Override
  public BytesInput getBytes() {
    //The Page Header should include: blockSizeInValues, numberOfMiniBlocks, totalValueCount
    if (deltaValuesToFlush != 0)
      flushWholeBlockBuffer();//TODO: bug, when getBytes is called multiple times
    return BytesInput.concat(
            config.toBytesInput(),
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
    this.deltaValuesToFlush = 0;
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
