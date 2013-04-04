package parquet.column.values.dictionary;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.BinaryEncodingPickerValuesWriter;
import parquet.io.api.Binary;

public class TestDictionary {

  @Test
  public void testDict() throws IOException {
    int COUNT = 100;
    DictionaryValuesWriter cw = new DictionaryValuesWriter();
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("a" + i % 10));
    }
    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    cw.reset();
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("b" + i % 10));
    }
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    cw.reset();

    final int dictionarySize = cw.getDictionarySize();
    final BytesInput dictionaryBytes = BytesInput.copy(cw.getDictionaryBytes());
    final DictionaryValuesReader cr = new DictionaryValuesReader();
    final Dictionary dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(
        new DictionaryPage(dictionaryBytes, dictionarySize, Encoding.PLAIN_DICTIONARY));
//    System.out.println(dictionary);
    cr.setDictionary(dictionary);

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      final String str = cr.readBytes().toStringUsingUTF8();
      Assert.assertEquals("a" + i % 10, str);
    }

    cr.initFromPage(COUNT, bytes2.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      final String str = cr.readBytes().toStringUsingUTF8();
      Assert.assertEquals("b" + i % 10, str);
    }

  }

  @Test
  public void testDictInefficiency() throws IOException {
    int COUNT = 100000;
    BinaryEncodingPickerValuesWriter cw = new BinaryEncodingPickerValuesWriter(2000000, 1100000);
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("a" + i ));
    }
    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    System.out.println(cw.getEncoding() + "  " + bytes1.size());
    cw.reset();
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("b" + i ));
    }
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    System.out.println(cw.getEncoding() + "  " + bytes2.size());
    cw.reset();

    final int dictionarySize = cw.getDictionarySize();
    final BytesInput dictionaryBytes = BytesInput.copy(cw.getDictionaryBytes());
    System.out.println("dict byte size: " + dictionaryBytes.size());
    final DictionaryValuesReader cr = new DictionaryValuesReader();
    final Dictionary dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(
        new DictionaryPage(dictionaryBytes, dictionarySize, Encoding.PLAIN_DICTIONARY));
//    System.out.println(dictionary);
    cr.setDictionary(dictionary);

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      final String str = cr.readBytes().toStringUsingUTF8();
      Assert.assertEquals("a" + i, str);
    }

    cr.initFromPage(COUNT, bytes2.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      final String str = cr.readBytes().toStringUsingUTF8();
      Assert.assertEquals("b" + i, str);
    }

  }
}
