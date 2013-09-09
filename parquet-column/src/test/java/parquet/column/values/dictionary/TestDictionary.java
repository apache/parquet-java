/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.column.values.dictionary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static parquet.column.Encoding.PLAIN_DICTIONARY;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import parquet.bytes.BytesInput;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.column.Encoding;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import parquet.column.values.plain.BinaryPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

public class TestDictionary {

  @Test
  public void testBinaryDictionary() throws IOException {

    int COUNT = 100;
    ValuesWriter cw = new PlainBinaryDictionaryValuesWriter(10000, 10000);
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("a" + i % 10));
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    cw.reset();
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("b" + i % 10));
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    cw.reset();

    final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, PrimitiveTypeName.BINARY, 0, 0);
    final Dictionary dictionary = PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);

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
  public void testBinaryDictionaryInefficiency() throws IOException {

    int COUNT = 40000;
    ValuesWriter cw = new PlainBinaryDictionaryValuesWriter(2000000, 10000);
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("a" + i ));
    }
    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    final Encoding encoding1 = cw.getEncoding();
    System.out.println(PrimitiveTypeName.BINARY.name() + " " + encoding1 + "  " + bytes1.size());
    cw.reset();
    for (int i = 0; i < COUNT; i++) {
      cw.writeBytes(Binary.fromString("b" + i ));
    }
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    final Encoding encoding2 = cw.getEncoding();
    System.out.println(PrimitiveTypeName.BINARY.name() + " " + encoding2 + "  " + bytes2.size());
    cw.reset();

    final DictionaryPage dictionaryPage = cw.createDictionaryPage();
    Dictionary dictionary = null;
    long dictPageSize = 0;
    ValuesReader cr;
    if (dictionaryPage != null) {
      dictPageSize = dictionaryPage.getBytes().size();
      System.out.println(PrimitiveTypeName.BINARY.name() + " dict byte size: " + dictPageSize);
      final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"foo"}, PrimitiveTypeName.BINARY, 0, 0);
      dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
      cr = new DictionaryValuesReader(dictionary);
    } else {
      cr = new BinaryPlainValuesReader();
    }

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      final String str = cr.readBytes().toStringUsingUTF8();
      Assert.assertEquals("a" + i, str);
    }

    if (dictionary != null && encoding2 == Encoding.PLAIN_DICTIONARY) {
      cr = new DictionaryValuesReader(dictionary);
    } else {
      cr = new BinaryPlainValuesReader();
    }
    cr.initFromPage(COUNT, bytes2.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      final String str = cr.readBytes().toStringUsingUTF8();
      Assert.assertEquals("b" + i, str);
    }

    assertTrue("encoded int values smaller (w/o considering dictionary size)", bytes1.size() < bytes2.size());
    assertEquals("dict page size", dictPageSize, bytes2.size()); // but dictionary is same size as full plain when no repeated values

  }

  @Test
  public void testLongDictionary() throws IOException {

    int COUNT = 1000;
    int COUNT2 = 2000;
    final DictionaryValuesWriter cw = new PlainLongDictionaryValuesWriter(10000, 10000);

    for (long i = 0; i < COUNT; i++) {
      cw.writeLong(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    cw.reset();

    for (long i = COUNT2; i > 0; i--) {
      cw.writeLong(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    cw.reset();

    final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"along"}, PrimitiveTypeName.INT64, 0, 0);
    final Dictionary dictionary = PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (long i = 0; i < COUNT; i++) {
      long back = cr.readLong();
      assertEquals(i % 50, back);
    }

    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
    for (long i = COUNT2; i > 0; i--) {
      long back = cr.readLong();
      assertEquals(i % 50, back);
    }

  }

  @Test
  public void testLongDictionaryInefficiency() throws IOException {

    int COUNT = 50000;
    final DictionaryValuesWriter cw = new PlainLongDictionaryValuesWriter(2000000, 10000);
    for (long i = 0; i < COUNT; i++) {
      cw.writeLong(i);
    }

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    final Encoding encoding1 = cw.getEncoding();
    System.out.println(PrimitiveTypeName.INT64.name() + " " + encoding1 + "  " + bytes1.size());

    final PlainValuesWriter pw = new PlainValuesWriter(64 * 1024);
    for (long i = 0; i < COUNT; i++) {
      pw.writeLong(i);
    }
    final BytesInput bytes2 = pw.getBytes();
    System.out.println(PrimitiveTypeName.INT64.name() + " " + pw.getEncoding() + "  " + bytes2.size());

    final DictionaryPage dictionaryPage = cw.createDictionaryPage();
    Dictionary dictionary = null;
    long dictPageSize = 0;
    ValuesReader cr;
    if (dictionaryPage != null) {
      dictPageSize = dictionaryPage.getBytes().size();
      System.out.println(PrimitiveTypeName.INT64.name() + " dict byte size: " + dictPageSize);
      final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"along"}, PrimitiveTypeName.INT64, 0, 0);
      dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
      cr = new DictionaryValuesReader(dictionary);
    } else {
      cr = new LongPlainValuesReader();
    }

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (long i = 0; i < COUNT; i++) {
      long back = cr.readLong();
      assertEquals(i, back);
    }

    assertTrue(bytes1.size() < bytes2.size()); // encoded int values smaller (w/o considering dictionary size)
    assertEquals(dictPageSize, bytes2.size()); // but dictionary is same size as full plain when no repeated values

  }

  @Test
  public void testDoubleDictionary() throws IOException {

    int COUNT = 1000;
    int COUNT2 = 2000;
    final DictionaryValuesWriter cw = new PlainDoubleDictionaryValuesWriter(10000, 10000);

    for (double i = 0; i < COUNT; i++) {
      cw.writeDouble(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    cw.reset();

    for (double i = COUNT2; i > 0; i--) {
      cw.writeDouble(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    cw.reset();

    final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"adouble"}, PrimitiveTypeName.DOUBLE, 0, 0);
    final Dictionary dictionary = PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (double i = 0; i < COUNT; i++) {
      double back = cr.readDouble();
      assertEquals(i % 50, back, 0.0);
    }

    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
    for (double i = COUNT2; i > 0; i--) {
      double back = cr.readDouble();
      assertEquals(i % 50, back, 0.0);
    }

  }

  @Test
  public void testDoubleDictionaryInefficiency() throws IOException {

    int COUNT = 30000;
    final DictionaryValuesWriter cw = new PlainDoubleDictionaryValuesWriter(2000000, 10000);
    for (double i = 0; i < COUNT; i++) {
      cw.writeDouble(i);
    }

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    final Encoding encoding1 = cw.getEncoding();
    System.out.println(PrimitiveTypeName.DOUBLE.name() + " " + encoding1 + "  " + bytes1.size());

    final PlainValuesWriter pw = new PlainValuesWriter(64 * 1024);
    for (double i = 0; i < COUNT; i++) {
      pw.writeDouble(i);
    }
    final BytesInput bytes2 = pw.getBytes();
    System.out.println(PrimitiveTypeName.DOUBLE.name() + " " + pw.getEncoding() + "  " + bytes2.size());

    final DictionaryPage dictionaryPage = cw.createDictionaryPage();
    Dictionary dictionary = null;
    long dictPageSize = 0;
    ValuesReader cr;
    if (dictionaryPage != null) {
      dictPageSize = dictionaryPage.getBytes().size();
      System.out.println(PrimitiveTypeName.DOUBLE.name() + " dict byte size: " + dictPageSize);
      final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"adouble"}, PrimitiveTypeName.DOUBLE, 0, 0);
      dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
      cr = new DictionaryValuesReader(dictionary);
    } else {
      cr = new DoublePlainValuesReader();
    }

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (double i = 0; i < COUNT; i++) {
      double back = cr.readDouble();
      assertEquals(i, back, 0.0);
    }

    assertTrue(bytes1.size() < bytes2.size()); // encoded int values smaller (w/o considering dictionary size)
    assertEquals(dictPageSize, bytes2.size()); // but dictionary is same size as full plain when no repeated values

  }

  @Test
  public void testIntDictionary() throws IOException {

    int COUNT = 2000;
    int COUNT2 = 4000;
    final DictionaryValuesWriter cw = new PlainIntegerDictionaryValuesWriter(10000, 10000);

    for (int i = 0; i < COUNT; i++) {
      cw.writeInteger(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    cw.reset();

    for (int i = COUNT2; i > 0; i--) {
      cw.writeInteger(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    cw.reset();

    final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"anint"}, PrimitiveTypeName.INT32, 0, 0);
    final Dictionary dictionary = PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      int back = cr.readInteger();
      assertEquals(i % 50, back);
    }

    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
    for (int i = COUNT2; i > 0; i--) {
      int back = cr.readInteger();
      assertEquals(i % 50, back);
    }

  }

  @Test
  public void testIntDictionaryInefficiency() throws IOException {

    int COUNT = 20000;
    final DictionaryValuesWriter cw = new PlainIntegerDictionaryValuesWriter(2000000, 10000);
    for (int i = 0; i < COUNT; i++) {
      cw.writeInteger(i);
    }

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    final Encoding encoding1 = cw.getEncoding();
    System.out.println(PrimitiveTypeName.INT32.name() + " " + encoding1 + "  " + bytes1.size());

    final PlainValuesWriter pw = new PlainValuesWriter(64 * 1024);
    for (int i = 0; i < COUNT; i++) {
      pw.writeInteger(i);
    }
    final BytesInput bytes2 = pw.getBytes();
    System.out.println(PrimitiveTypeName.INT32.name() + " " + pw.getEncoding() + "  " + bytes2.size());

    final DictionaryPage dictionaryPage = cw.createDictionaryPage();
    Dictionary dictionary = null;
    long dictPageSize = 0;
    ValuesReader cr;
    if (dictionaryPage != null) {
      dictPageSize = dictionaryPage.getBytes().size();
      System.out.println(PrimitiveTypeName.INT32.name() + " dict byte size: " + dictPageSize);
      final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"anint"}, PrimitiveTypeName.INT32, 0, 0);
      dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
      cr = new DictionaryValuesReader(dictionary);
    } else {
      cr = new IntegerPlainValuesReader();
    }

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (int i = 0; i < COUNT; i++) {
      int back = cr.readInteger();
      assertEquals(i, back);
    }

    assertTrue(bytes1.size() < bytes2.size()); // encoded int values smaller (w/o considering dictionary size)
    assertEquals(dictPageSize, bytes2.size()); // but dictionary is same size as full plain when no repeated values

  }

  @Test
  public void testFloatDictionary() throws IOException {

    int COUNT = 2000;
    int COUNT2 = 4000;
    final DictionaryValuesWriter cw = new PlainFloatDictionaryValuesWriter(10000, 10000);

    for (float i = 0; i < COUNT; i++) {
      cw.writeFloat(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    cw.reset();

    for (float i = COUNT2; i > 0; i--) {
      cw.writeFloat(i % 50);
    }
    assertEquals(PLAIN_DICTIONARY, cw.getEncoding());
    assertEquals(50, cw.getDictionarySize());
    final BytesInput bytes2 = BytesInput.copy(cw.getBytes());
    cw.reset();

    final DictionaryPage dictionaryPage = cw.createDictionaryPage().copy();
    final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"afloat"}, PrimitiveTypeName.FLOAT, 0, 0);
    final Dictionary dictionary = PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
    final DictionaryValuesReader cr = new DictionaryValuesReader(dictionary);

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (float i = 0; i < COUNT; i++) {
      float back = cr.readFloat();
      assertEquals(i % 50, back, 0.0f);
    }

    cr.initFromPage(COUNT2, bytes2.toByteArray(), 0);
    for (float i = COUNT2; i > 0; i--) {
      float back = cr.readFloat();
      assertEquals(i % 50, back, 0.0f);
    }

  }

  @Test
  public void testFloatDictionaryInefficiency() throws IOException {

    int COUNT = 60000;
    final DictionaryValuesWriter cw = new PlainFloatDictionaryValuesWriter(2000000, 10000);
    for (float i = 0; i < COUNT; i++) {
      cw.writeFloat(i);
    }

    final BytesInput bytes1 = BytesInput.copy(cw.getBytes());
    final Encoding encoding1 = cw.getEncoding();
    System.out.println(PrimitiveTypeName.FLOAT.name() + " " + encoding1 + "  " + bytes1.size());

    final PlainValuesWriter pw = new PlainValuesWriter(64 * 1024);
    for (float i = 0; i < COUNT; i++) {
      pw.writeFloat(i);
    }
    final BytesInput bytes2 = pw.getBytes();
    System.out.println(PrimitiveTypeName.FLOAT.name() + " " + pw.getEncoding() + "  " + bytes2.size());

    final DictionaryPage dictionaryPage = cw.createDictionaryPage();
    Dictionary dictionary = null;
    long dictPageSize = 0;
    ValuesReader cr;
    if (dictionaryPage != null) {
      dictPageSize = dictionaryPage.getBytes().size();
      System.out.println(PrimitiveTypeName.FLOAT.name() + " dict byte size: " + dictPageSize);
      final ColumnDescriptor descriptor = new ColumnDescriptor(new String[] {"afloat"}, PrimitiveTypeName.FLOAT, 0, 0);
      dictionary = Encoding.PLAIN_DICTIONARY.initDictionary(descriptor, dictionaryPage);
      cr = new DictionaryValuesReader(dictionary);
    } else {
      cr = new FloatPlainValuesReader();
    }

    cr.initFromPage(COUNT, bytes1.toByteArray(), 0);
    for (float i = 0; i < COUNT; i++) {
      float back = cr.readFloat();
      assertEquals(i, back, 0.0f);
    }

    assertTrue(bytes1.size() < bytes2.size()); // encoded int values smaller (w/o considering dictionary size)
    assertEquals(dictPageSize, bytes2.size()); // but dictionary is same size as full plain when no repeated values

  }
}
