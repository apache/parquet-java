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
package parquet.column;

import static parquet.column.values.bitpacking.Packer.BIG_ENDIAN;

import java.io.IOException;

import parquet.bytes.BytesUtils;
import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;
import parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import parquet.column.values.boundedint.ZeroIntegerValuesReader;
import parquet.column.values.dictionary.DictionaryValuesReader;
import parquet.column.values.dictionary.PlainValuesDictionary.PlainBinaryDictionary;
import parquet.column.values.dictionary.PlainValuesDictionary.PlainDoubleDictionary;
import parquet.column.values.dictionary.PlainValuesDictionary.PlainFloatDictionary;
import parquet.column.values.dictionary.PlainValuesDictionary.PlainIntegerDictionary;
import parquet.column.values.dictionary.PlainValuesDictionary.PlainLongDictionary;
import parquet.column.values.plain.BinaryPlainValuesReader;
import parquet.column.values.plain.BooleanPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import parquet.io.ParquetDecodingException;

/**
 * encoding of the data
 *
 * @author Julien Le Dem
 *
 */
public enum Encoding {

  PLAIN {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      switch (descriptor.getType()) {
      case BOOLEAN:
        return new BooleanPlainValuesReader();
      case BINARY:
        return new BinaryPlainValuesReader();
      case FLOAT:
        return new FloatPlainValuesReader();
      case DOUBLE:
        return new DoublePlainValuesReader();
      case INT32:
        return new IntegerPlainValuesReader();
      case INT64:
        return new LongPlainValuesReader();
      case FIXED_LEN_BYTE_ARRAY:
        String[] path = descriptor.getPath();
        // XXX Pass the type length into the constructor for the values reader.
        System.out.println("Encoding.getValuesReader FIXED type field " + path[path.length - 1] + 
                           " length: " + descriptor.getTypeLength());
        //return new FixedLenByteArrayPlainValuesReader();
      default:
        throw new ParquetDecodingException("no plain reader for type " + descriptor.getType());
      }
    }
  },

  /**
   * Actually a combination of bit packing and run length encoding.
   * TODO: Should we rename this to be more clear?
   */
  RLE {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
      if(bitWidth == 0) {
        return new ZeroIntegerValuesReader();
      }
      return new RunLengthBitPackingHybridValuesReader(bitWidth);
    }
  },

  /**
   * This is no longer used, and has been replaced by {@link #RLE}
   * which is combination of bit packing and rle
   */
  @Deprecated
  BIT_PACKED {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      return new ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), BIG_ENDIAN);
    }
  },

  GROUP_VAR_INT {
    @Override // TODO: GROUP VAR INT encoding
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      throw new UnsupportedOperationException("NYI");
    }
  },

  PLAIN_DICTIONARY {
    @Override
    public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
      switch (descriptor.getType()) {
      case BINARY:
      case INT64:
      case DOUBLE:
      case INT32:
      case FLOAT:
        return new DictionaryValuesReader(dictionary);
      default:
        throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
      }
    }

    @Override
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
      switch (descriptor.getType()) {
      case BINARY:
        return new PlainBinaryDictionary(dictionaryPage);
      case INT64:
        return new PlainLongDictionary(dictionaryPage);
      case DOUBLE:
        return new PlainDoubleDictionary(dictionaryPage);
      case INT32:
        return new PlainIntegerDictionary(dictionaryPage);
      case FLOAT:
        return new PlainFloatDictionary(dictionaryPage);
      default:
        throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
      }
      
    }

    @Override
    public boolean usesDictionary() {
      return true;
    }

  };

  int getMaxLevel(ColumnDescriptor descriptor, ValuesType valuesType) {
    int maxLevel;
    switch (valuesType) {
    case REPETITION_LEVEL:
      maxLevel = descriptor.getMaxRepetitionLevel();
      break;
    case DEFINITION_LEVEL:
      maxLevel = descriptor.getMaxDefinitionLevel();
      break;
    default:
      throw new ParquetDecodingException("Unsupported encoding for values: " + this);
    }
    return maxLevel;
  }

  /**
   * @return whether this encoding requires a dictionary
   */
  public boolean usesDictionary() {
    return false;
  }

  /**
   * initializes a dictionary from a page
   * @param dictionaryPage
   * @return the corresponding dictionary
   */
  public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
    throw new UnsupportedOperationException(this.name() + " does not support dictionary");
  }

  /**
   * To read decoded values that don't require a dictionary
   *
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @return the proper values reader for the given column
   * @throw {@link UnsupportedOperationException} if the encoding is dictionary based
   */
  public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
    throw new UnsupportedOperationException("Error decoding " + descriptor + ". " + this.name() + " is dictionary based");
  }

  /**
   * To read decoded values that require a dictionary
   *
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @param dictionary the dictionary
   * @return the proper values reader for the given column
   * @throw {@link UnsupportedOperationException} if the encoding is not dictionary based
   */
  public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary) {
    throw new UnsupportedOperationException(this.name() + " is not dictionary based");
  }

}
