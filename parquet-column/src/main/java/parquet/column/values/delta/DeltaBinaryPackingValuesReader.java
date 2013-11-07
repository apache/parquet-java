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
  private int firstValue;
  private int previousValue;
  private int minDeltaInCurrentBlock;
  private int miniBlockSizeInValues;
  private int[] currentBlockBuffer;
  private int numberBuffered = 0;
  private ByteArrayInputStream in;

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset) throws IOException {
    in = new ByteArrayInputStream(page, offset, page.length - offset);
    this.blockSizeInValues = BytesUtils.readIntLittleEndian(in);
    this.miniBlockNum = BytesUtils.readIntLittleEndian(in);
    this.totalValueCount = BytesUtils.readIntLittleEndian(in);
    this.firstValue = BytesUtils.readIntLittleEndian(in);
    this.previousValue = firstValue;
    this.miniBlockSizeInValues = blockSizeInValues / miniBlockNum;
    currentBlockBuffer = new int[blockSizeInValues];
    return 0;//TODO: return offset
  }

  @Override
  public void skip() {
  }

  @Override
  public int readInteger() {
    if (totalValueCount == valuesRead)
      throw new ParquetDecodingException("no more value to read, total value count is " + totalValueCount);
    if (numberBuffered == 0)
      loadNewBlock();
    valuesRead++;
    int currentValue = previousValue + currentBlockBuffer[blockSizeInValues - (numberBuffered--)];
    previousValue = currentValue;
    return currentValue;
  }

  private void loadNewBlock() {
    try {
      minDeltaInCurrentBlock = BytesUtils.readIntLittleEndian(in);
    } catch (IOException e) {
      throw new ParquetDecodingException("can not read min delta in current block");
    }

    int[] bitWiths = new int[miniBlockNum];
    readBitWidthsForMiniBlocks(bitWiths);
    for (int i = 0; i < miniBlockNum; i++) {
      int currentBitWidth = bitWiths[i];
      BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(currentBitWidth);

      byte[] bytes = new byte[currentBitWidth];
      for (int j = 0; j < miniBlockSizeInValues; j += 8) {
        try {
          in.read(bytes);
        } catch (IOException e) {
          throw new ParquetDecodingException("can not read mini block", e);
        }
        int offset = i * miniBlockSizeInValues + j;
        unpack8Values(packer, bytes, offset);
      }
    }
    numberBuffered = blockSizeInValues;
  }

  private void unpack8Values(BytePacker packer, byte[] bytes, int offset) {
    if (packer.getBitWidth() == 0) {
      for (int i = 0; i < 8; i++) {
        currentBlockBuffer[offset + i] = 0 + minDeltaInCurrentBlock;
      }
    } else {
      packer.unpack8Values(bytes, 0, currentBlockBuffer, offset);
      for (int i = 0; i < 8; i++) {
        currentBlockBuffer[offset + i] += minDeltaInCurrentBlock;
      }

    }
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
