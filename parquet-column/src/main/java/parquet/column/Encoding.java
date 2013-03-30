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

import parquet.column.values.ValuesReader;
import parquet.column.values.ValuesType;
import parquet.column.values.bitpacking.BitPackingValuesReader;
import parquet.column.values.boundedint.BoundedIntValuesFactory;
import parquet.column.values.plain.BinaryPlainValuesReader;
import parquet.column.values.plain.BooleanPlainValuesReader;
import parquet.column.values.plain.PlainValuesReader;
import parquet.io.ParquetDecodingException;

/**
 * endoding of the data
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

  RLE {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      return BoundedIntValuesFactory.getBoundedReader(getMaxLevel(descriptor, valuesType));
    }
  },

  BIT_PACKED {
    @Override
    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType) {
      return new BitPackingValuesReader(getMaxLevel(descriptor, valuesType));
    }
  },

  GROUP_VAR_INT {

    @Override // TODO: GROUP VAR INT encoding
    public ValuesReader getValuesReader(ColumnDescriptor descriptor,
        ValuesType valuesType) {
      throw new UnsupportedOperationException("NYI");
    }

  },

  PLAIN_DICTIONARY {

    @Override // TODO: dictionary encoding
    public ValuesReader getValuesReader(ColumnDescriptor descriptor,
        ValuesType valuesType) {
      throw new UnsupportedOperationException("NYI");
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
   * @param descriptor the column to read
   * @param valuesType the type of values
   * @return the proper values reader for the given column
   */
  abstract public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType);
}
