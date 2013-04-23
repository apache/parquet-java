package parquet.column.values.dictionary;

import static parquet.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import parquet.Log;
import parquet.column.Dictionary;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

public class DictionaryValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(DictionaryValuesReader.class);

  private InputStream in;

  private Dictionary dictionary;

  @Override
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (page.length - offset));
    this.in = new ByteArrayInputStream(page, offset, page.length - offset);
    return page.length;
  }

  @Override
  public Binary readBytes() {
    try {
      int ch4 = in.read();
      int ch3 = in.read();
      if ((ch3 | ch4) < 0)
        throw new EOFException();
      int id = ((ch3 << 8) + (ch4 << 0));
      return dictionary.decodeToBinary(id);
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read bytes", e);
    }
  }

}
