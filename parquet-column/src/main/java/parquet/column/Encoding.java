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

import parquet.column.page.DictionaryPage;
import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesType;
import parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import parquet.column.values.bitpacking.Packer;
import parquet.column.values.boundedint.BoundedIntValuesFactory;
import parquet.column.values.dictionary.DictionaryValuesReader;
import parquet.column.values.dictionary.PlainBinaryDictionary;
import parquet.column.values.plain.BinaryPlainValuesReader;
import parquet.column.values.plain.BooleanPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader;
import parquet.io.ParquetDecodingException;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

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
      default:
        return new PlainValuesReader();
      }
    }
  },

  /**
   * this is not used anymore and will be removed
   */
  @Deprecated
  RLE {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      return BoundedIntValuesFactory.getBoundedReader(getMaxLevel(descriptor, valuesType));
    }
  },

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
        return new DictionaryValuesReader(dictionary);
      default:
        throw new ParquetDecodingException("Dictionary encoding not supported for type: " + descriptor.getType());
      }
    }

    @Override
    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage) throws IOException {
      if (descriptor.getType() != PrimitiveTypeName.BINARY) {
        throw new UnsupportedOperationException("only Binary dictionaries are supported for now");
      }
      return new PlainBinaryDictionary(dictionaryPage);
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
