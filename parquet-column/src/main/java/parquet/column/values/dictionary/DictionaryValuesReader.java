package parquet.column.values.dictionary;

import static parquet.Log.DEBUG;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import parquet.Log;
import parquet.bytes.BytesUtils;
import parquet.column.Dictionary;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RLEDecoder;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

/**
 * Reads values that have been dictionary encoded
 *
 * @author Julien Le Dem
 *
 */
public class DictionaryValuesReader extends ValuesReader {
  private static final Log LOG = Log.getLog(DictionaryValuesReader.class);

  private InputStream in;

  private Dictionary dictionary;

  private RLEDecoder decoder;

  public DictionaryValuesReader(Dictionary dictionary) {
    this.dictionary = dictionary;
  }

  @Override
  public int initFromPage(long valueCount, byte[] page, int offset)
      throws IOException {
    if (DEBUG) LOG.debug("init from page at offset "+ offset + " for length " + (page.length - offset));
    this.in = new ByteArrayInputStream(page, offset, page.length - offset);
    int maxDicId = BytesUtils.readIntLittleEndianOnTwoBytes(in);
    if (DEBUG) LOG.debug("max dic Id " + maxDicId);
    decoder = new RLEDecoder(BytesUtils.getWidthFromMaxInt(maxDicId), in);
    return page.length;
  }

  @Override
  public int readValueDictionaryId() {
    try {
      return decoder.readInt();
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public Binary readBytes() {
    try {
      return dictionary.decodeToBinary(decoder.readInt());
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

}
