package parquet.column.values.delta;


import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.BytePacker;
import parquet.column.values.bitpacking.Packer;
import parquet.io.ParquetDecodingException;

import java.io.ByteArrayInputStream;
import java.io.IOException;


public class DeltaBinaryPackingValuesReader extends ValuesReader {
  private int blockSizeInValues;
  private int miniBlockNum;
  private int totalValueCount;
  private int valuesRead;
  private int previousValue;
  private int minDeltaInCurrentBlock;
  private int miniBlockSizeInValues;
  private byte[] page;
  private int[] totalValueBuffer;
  private int valuesBuffered;
  private ByteArrayInputStream in;

  /**
   * eagerly load all the data into memory
   *
   * @param valueCount count of values in this page
   * @param page       the array to read from containing the page data (repetition levels, definition levels, data)
   * @param offset     where to start reading from in the page
   * @return
   * @throws IOException
   */
  @Override
  public int initFromPage(long valueCount, byte[] page, int offset) throws IOException {
    in = new ByteArrayInputStream(page, offset, page.length - offset); //TODO use var int
    this.page = page;
    this.blockSizeInValues = BytesUtils.readUnsignedVarInt(in);
    this.miniBlockNum = BytesUtils.readUnsignedVarInt(in);
    this.totalValueCount = BytesUtils.readUnsignedVarInt(in);
    this.previousValue = BytesUtils.readUnsignedVarInt(in);
    this.miniBlockSizeInValues = blockSizeInValues / miniBlockNum;
    assert (miniBlockSizeInValues % 8 == 0) : "miniBlockSize must be multiple of 8"; //TODO use Precondition

    int totalMiniBlockCount = (int) Math.ceil((double) totalValueCount / miniBlockSizeInValues);
    totalValueBuffer = new int[totalMiniBlockCount * miniBlockSizeInValues];//TODO: this buffer should be of size which is multiple of size of miniBuffer

    while (valuesBuffered < totalValueCount) { //values Buffered could be more than totalValueCount, since we flush on a mini block basis
      loadNewBlock();
//      System.out.println("load new block");
    }
    return page.length - in.available() - offset;
  }

  @Override
  public void skip() {
    valuesRead++;
  }

  @Override
  public int readInteger() {
    if (totalValueCount == valuesRead)
      throw new ParquetDecodingException("no more value to read, total value count is " + totalValueCount);
    return totalValueBuffer[valuesRead++];
  }

  private void loadNewBlock() {
    try {
      minDeltaInCurrentBlock = BytesUtils.readUnsignedVarInt(in); //TODO var int
    } catch (IOException e) {
      throw new ParquetDecodingException("can not read min delta in current block");
    }

    int[] bitWiths = new int[miniBlockNum];
    readBitWidthsForMiniBlocks(bitWiths);//this is ok


    for (int i = 0; i < miniBlockNum; i++) {
      int currentBitWidth = bitWiths[i];
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(currentBitWidth);

      byte[] bytes = new byte[currentBitWidth];
      for (int j = 0; j < miniBlockSizeInValues && valuesBuffered < totalValueCount; j += 8) { // mini block is atomic for reading, we read a mini block when there are more values left
        unpack8Values(packer, bytes, valuesBuffered);
      }

    }
  }

  private void unpack8Values(BytePacker packer, byte[] bytes, int offset) {
    valuesBuffered += 8;
    if (packer.getBitWidth() == 0) {
      for (int i = 0; i < 8; i++) {
        totalValueBuffer[offset + i] = 0 + minDeltaInCurrentBlock + previousValue;
        previousValue = totalValueBuffer[offset + i];
      }
    } else {
      int pos = page.length - in.available();
      packer.unpack8Values(page, pos, totalValueBuffer, offset);
      for (int i = 0; i < 8; i++) {
        totalValueBuffer[offset + i] += minDeltaInCurrentBlock + previousValue;
        previousValue = totalValueBuffer[offset + i];
      }

    }
    in.skip(packer.getBitWidth());
  }

  private void readBitWidthsForMiniBlocks(int[] bitWiths) {
    for (int i = 0; i < miniBlockNum; i++) {
      try {
        bitWiths[i] = BytesUtils.readIntLittleEndianOnOneByte(in);
      } catch (IOException e) {
        throw new ParquetDecodingException("Can not decode bitwith in block header", e);
      }
    }
  }
}
