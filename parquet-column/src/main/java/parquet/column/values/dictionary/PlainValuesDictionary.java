/** Copyright 2012 Twitter, Inc.
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

import static parquet.bytes.BytesUtils.readIntLittleEndian;
import static parquet.column.Encoding.PLAIN_DICTIONARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.IOException;

import parquet.column.Dictionary;
import parquet.column.page.DictionaryPage;
import parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

/**
 * 
 * a simple implementation of dictionary for plain encoded binary, long, double, int and float dictionaries
 *
 */
public class PlainValuesDictionary extends Dictionary {

  private PrimitiveTypeName dictionaryType;
  
  private Binary[] binaryDictionaryContent = null;
  private int[] intDictionaryContent = null;
  private long[] longDictionaryContent = null;
  private double[] doubleDictionaryContent = null;
  private float[] floatDictionaryContent = null;

  /**
   * @param dictionaryPage the PLAIN encoded content of the dictionary
   * @throws IOException
   */
  public PlainValuesDictionary(PrimitiveTypeName dictionaryType, DictionaryPage dictionaryPage) throws IOException {
    super(dictionaryPage.getEncoding());
    if (dictionaryPage.getEncoding() != PLAIN_DICTIONARY) {
      throw new ParquetDecodingException("Dictionary data encoding not supported: " + dictionaryPage.getEncoding());
    }
    this.dictionaryType = dictionaryType;
    final byte[] dictionaryBytes = dictionaryPage.getBytes().toByteArray();
    switch(dictionaryType) {
    case BINARY:
      binaryDictionaryContent = new Binary[dictionaryPage.getDictionarySize()];
      // dictionary values are stored in order: size (4 bytes LE) followed by {size} bytes
      int offset = 0;
      for (int i = 0; i < binaryDictionaryContent.length; i++) {
        int length = readIntLittleEndian(dictionaryBytes, offset);
        // read the length
        offset += 4;
        // wrap the content in a binary
        binaryDictionaryContent[i] = Binary.fromByteArray(dictionaryBytes, offset, length);
        // increment to the next value
        offset += length;
      }
      break;
    case INT64:
      longDictionaryContent = new long[dictionaryPage.getDictionarySize()];
      LongPlainValuesReader longReader = new LongPlainValuesReader();
      longReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < longDictionaryContent.length; i++) {
        longDictionaryContent[i] = longReader.readLong();
      }
      break;
    case DOUBLE:
      doubleDictionaryContent = new double[dictionaryPage.getDictionarySize()];
      DoublePlainValuesReader doubleReader = new DoublePlainValuesReader();
      doubleReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        doubleDictionaryContent[i] = doubleReader.readDouble();
      }
      break;
    case INT32:
      intDictionaryContent = new int[dictionaryPage.getDictionarySize()];
      IntegerPlainValuesReader intReader = new IntegerPlainValuesReader();
      intReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < intDictionaryContent.length; i++) {
        intDictionaryContent[i] = intReader.readInteger();
      }
      break;
    case FLOAT:
      floatDictionaryContent = new float[dictionaryPage.getDictionarySize()];
      FloatPlainValuesReader floatReader = new FloatPlainValuesReader();
      floatReader.initFromPage(dictionaryPage.getDictionarySize(), dictionaryBytes, 0);
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        floatDictionaryContent[i] = floatReader.readFloat();
      }
      break;
      default:
        throw new ParquetDecodingException("Dictionary type not supported: " + dictionaryType);
    }  
  }

  @Override
  public Binary decodeToBinary(int id) {
    if (dictionaryType != BINARY) {
      throw new UnsupportedOperationException("cannot decodeToBinary from a " + dictionaryType + " dictionary");
    }
    return binaryDictionaryContent[id];
  }

  
  @Override
  public int decodeToInt(int id) {
    if (dictionaryType != INT32) {
      throw new UnsupportedOperationException("cannot decodeToInt from a " + dictionaryType + " dictionary");
    }
    return intDictionaryContent[id];
  }

  @Override
  public long decodeToLong(int id) {
    if (dictionaryType != INT64) {
      throw new UnsupportedOperationException("cannot decodeToLong from a " + dictionaryType + " dictionary");
    }
    return longDictionaryContent[id];
  }

  @Override
  public float decodeToFloat(int id) {
    if (dictionaryType != FLOAT) {
      throw new UnsupportedOperationException("cannot decodeToFloat from a " + dictionaryType + " dictionary");
    }
    return floatDictionaryContent[id];
  }

  @Override
  public double decodeToDouble(int id) {
    if (dictionaryType != DOUBLE) {
      throw new UnsupportedOperationException("cannot decodeToDouble from a " + dictionaryType + " dictionary");
    }
    return doubleDictionaryContent[id];
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PlainValuesDictionary {\n");
    switch(dictionaryType) {
    case BINARY:
      for (int i = 0; i < binaryDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(binaryDictionaryContent[i]).append("\n");
      }
      break;
    case INT64:
      for (int i = 0; i < longDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(longDictionaryContent[i]).append("\n");
      }
      break;
    case INT32:
      for (int i = 0; i < intDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(intDictionaryContent[i]).append("\n");
      }
      break;
    case FLOAT:
      for (int i = 0; i < floatDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(floatDictionaryContent[i]).append("\n");
      }
      break;
    case DOUBLE:
      for (int i = 0; i < doubleDictionaryContent.length; i++) {
        sb.append(i).append(" => ").append(doubleDictionaryContent[i]).append("\n");
      }
      break;
    default:
    }
    return sb.append("}").toString();
  }

  @Override
  public int getMaxId() {
    switch(dictionaryType) {
    case BINARY: 
      return binaryDictionaryContent.length -1;
    case INT64: 
      return longDictionaryContent.length -1;   
    case INT32: 
      return intDictionaryContent.length -1;
    case FLOAT: 
      return floatDictionaryContent.length -1;
    case DOUBLE: 
      return doubleDictionaryContent.length -1;
    default: 
      throw new ParquetDecodingException("failed to determine maxId for dictionary of type: " + dictionaryType);
    }
  }

}
