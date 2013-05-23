package parquet.column.values.dictionary;

import static parquet.bytes.BytesUtils.readIntLittleEndian;
import static parquet.column.Encoding.PLAIN_DICTIONARY;

import java.io.IOException;

import parquet.bytes.BytesUtils;
import parquet.column.Dictionary;
import parquet.column.page.DictionaryPage;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

/**
 * a simple implementation of dictionary for Binary data
 *
 * @author Julien Le Dem
 *
 */
public class PlainBinaryDictionary extends Dictionary {

  private final Binary[] dictionaryData;

  /**
   * @param dictionaryPage the PLAIN encoded content of the dictionary
   * @throws IOException
   */
  public PlainBinaryDictionary(DictionaryPage dictionaryPage) throws IOException {
    super(dictionaryPage.getEncoding());
    if (dictionaryPage.getEncoding() != PLAIN_DICTIONARY) {
      throw new ParquetDecodingException("Dictionary encoding not supported: " + dictionaryPage.getEncoding());
    }
    final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
    dictionaryData = new Binary[dictionaryPage.getDictionarySize()];
    // dictionary values are stored in order: size (4 bytes LE) followed by {size} bytes
    int offset = 0;
    for (int i = 0; i < dictionaryData.length; i++) {
      int length = readIntLittleEndian(dictionaryBytes, offset);
      // read the length
      offset += 4;
      // wrap the content in a binary
      dictionaryData[i] = Binary.fromByteArray(dictionaryBytes, offset, length);
      // increment to the next value
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
