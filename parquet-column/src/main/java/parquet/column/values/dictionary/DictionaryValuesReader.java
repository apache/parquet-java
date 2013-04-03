package parquet.column.values.dictionary;

import static parquet.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import parquet.Log;
import parquet.bytes.LittleEndianDataInputStream;
import parquet.column.Dictionary;
import parquet.column.values.ValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

public class DictionaryValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(DictionaryValuesReader.class);

  private LittleEndianDataInputStream in;

  private Dictionary dictionary;

  @Override
  public void setDictionary(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (page.length - offset));
    this.in = new LittleEndianDataInputStream(new ByteArrayInputStream(page, offset, page.length - offset));
    return page.length;
  }

  @Override
  public Binary readBytes() {
    try {
      return dictionary.decode(in.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException("could not read bytes", e);
    }
  }

}
