package parquet.column.values.dictionary;

import java.io.IOException;

import parquet.bytes.BytesUtils;
import parquet.column.Dictionary;
import parquet.column.page.DictionaryPage;
import parquet.io.api.Binary;

public class PlainBinaryDictionary extends Dictionary {

  private final Binary[] dictionaryData;

  public PlainBinaryDictionary(DictionaryPage dictionaryPage) throws IOException {
    super(dictionaryPage.getEncoding());
    final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
    dictionaryData = new Binary[dictionaryPage.getDictionarySize()];
    int offset = 0;
    for (int i = 0; i < dictionaryData.length; i++) {
      int length = BytesUtils.readIntLittleEndian(dictionaryBytes, offset);
      offset += 4;
      dictionaryData[i] = Binary.fromByteArray(dictionaryBytes, offset, length);
      offset += length;
    }
  }

  @Override
  public Binary decodeToBinary(int id) {
    return dictionaryData[id];
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PlainDictionary {\n");
    for (int i = 0; i < dictionaryData.length; i++) {
      Binary element = dictionaryData[i];
      sb.append(i).append(" => ").append(element.toStringUsingUTF8()).append("\n");
    }
    return sb.append("}").toString();
  }

  @Override
  public int getMaxId() {
    return dictionaryData.length - 1;
  }

}
