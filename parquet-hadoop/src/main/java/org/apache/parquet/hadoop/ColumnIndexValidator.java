/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.hadoop;

import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.MAX_ASCENDING;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.MAX_DESCENDING;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.MAX_GTEQ_VALUE;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.MIN_ASCENDING;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.MIN_DESCENDING;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.MIN_LTEQ_VALUE;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.NULL_COUNT_CORRECT;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.NULL_PAGE_HAS_NO_MAX;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.NULL_PAGE_HAS_NO_MIN;
import static org.apache.parquet.hadoop.ColumnIndexValidator.Contract.NULL_PAGE_HAS_NO_VALUES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;

public class ColumnIndexValidator {

  public enum Contract {
    MIN_LTEQ_VALUE(
        "The min value stored in the index for the page must be less than or equal to all values in the page.\n"
            + "Actual value in the page: %s\n"
            + "Min value in the index: %s\n"),
    MAX_GTEQ_VALUE(
        "The max value stored in the index for the page must be greater than or equal to all values in the page.\n"
            + "Actual value in the page: %s\n"
            + "Max value in the index: %s\n"),
    NULL_COUNT_CORRECT(
        "The null count stored in the index for the page must be equal to the number of nulls in the page.\n"
            + "Actual null count: %s\n"
            + "Null count in the index: %s\n"),
    NULL_PAGE_HAS_NO_VALUES("Only pages consisting entirely of NULL-s can be marked as a null page in the index.\n"
        + "Actual non-null value in the page: %s"),
    NULL_PAGE_HAS_NO_MIN("A null page shall not have a min value in the index\n"
        + "Min value in the index: %s\n"),
    NULL_PAGE_HAS_NO_MAX("A null page shall not have a max value in the index\n"
        + "Max value in the index: %s\n"),
    MIN_ASCENDING(
        "According to the ASCENDING boundary order, the min value for a page must be greater than or equal to the min value of the previous page.\n"
            + "Min value for the page: %s\n"
            + "Min value for the previous page: %s\n"),
    MAX_ASCENDING(
        "According to the ASCENDING boundary order, the max value for a page must be greater than or equal to the max value of the previous page.\n"
            + "Max value for the page: %s\n"
            + "Max value for the previous page: %s\n"),
    MIN_DESCENDING(
        "According to the DESCENDING boundary order, the min value for a page must be less than or equal to the min value of the previous page.\n"
            + "Min value for the page: %s\n"
            + "Min value for the previous page: %s\n"),
    MAX_DESCENDING(
        "According to the DESCENDING boundary order, the max value for a page must be less than or equal to the max value of the previous page.\n"
            + "Max value for the page: %s\n"
            + "Max value for the previous page: %s\n");

    public final String description;

    Contract(String description) {
      this.description = description;
    }
  }

  public static class ContractViolation {
    public ContractViolation(Contract violatedContract, String referenceValue, String offendingValue,
        int rowGroupNumber, int columnNumber, ColumnPath columnPath, int pageNumber) {
      this.violatedContract = violatedContract;
      this.referenceValue = referenceValue;
      this.offendingValue = offendingValue;
      this.rowGroupNumber = rowGroupNumber;
      this.columnNumber = columnNumber;
      this.columnPath = columnPath;
      this.pageNumber = pageNumber;
    }

    private final Contract violatedContract;
    private final String referenceValue;
    private final String offendingValue;
    private final int rowGroupNumber;
    private final int columnNumber;
    private final ColumnPath columnPath;
    private final int pageNumber;

    @Override
    public String toString() {
      return String.format(
          "Contract violation\nLocation: row group %d, column %d (\"%s\"), page %d\nViolated contract: "
              + violatedContract.description,
          rowGroupNumber, columnNumber, columnPath.toDotString(), pageNumber,
          referenceValue,
          offendingValue);
    }
  }

  static interface StatValue extends Comparable<StatValue> {
    int compareToValue(ColumnReader reader);

    abstract class Builder {
      final PrimitiveComparator<Binary> comparator;
      final PrimitiveStringifier stringifier;

      Builder(PrimitiveType type) {
        comparator = type.comparator();
        stringifier = type.stringifier();
      }

      abstract StatValue build(ByteBuffer value);

      abstract String stringifyValue(ColumnReader reader);
    }
  }

  static StatValue.Builder getBuilder(PrimitiveType type) {
    switch (type.getPrimitiveTypeName()) {
    case BINARY:
    case FIXED_LEN_BYTE_ARRAY:
    case INT96:
      return new BinaryStatValueBuilder(type);
    case BOOLEAN:
      return new BooleanStatValueBuilder(type);
    case DOUBLE:
      return new DoubleStatValueBuilder(type);
    case FLOAT:
      return new FloatStatValueBuilder(type);
    case INT32:
      return new IntStatValueBuilder(type);
    case INT64:
      return new LongStatValueBuilder(type);
    default:
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }

  private static class BinaryStatValueBuilder extends StatValue.Builder {
    private class Value implements StatValue {
      final Binary value;

      private Value(Binary value) {
        this.value = value;
      }

      @Override
      public int compareTo(StatValue o) {
        return comparator.compare(value, ((Value) o).value);
      }

      @Override
      public int compareToValue(ColumnReader reader) {
        return comparator.compare(value, reader.getBinary());
      }

      @Override
      public String toString() {
        return stringifier.stringify(value);
      }
    }

    private BinaryStatValueBuilder(PrimitiveType type) {
      super(type);
    }

    @Override
    StatValue build(ByteBuffer value) {
      return new Value(Binary.fromConstantByteBuffer(value));
    }

    @Override
    String stringifyValue(ColumnReader reader) {
      return stringifier.stringify(reader.getBinary());
    }
  }

  private static class BooleanStatValueBuilder extends StatValue.Builder {
    private class Value implements StatValue {
      final boolean value;

      private Value(boolean value) {
        this.value = value;
      }

      @Override
      public int compareTo(StatValue o) {
        return comparator.compare(value, ((Value) o).value);
      }

      @Override
      public int compareToValue(ColumnReader reader) {
        return comparator.compare(value, reader.getBoolean());
      }

      @Override
      public String toString() {
        return stringifier.stringify(value);
      }
    }

    private BooleanStatValueBuilder(PrimitiveType type) {
      super(type);
    }

    @Override
    StatValue build(ByteBuffer value) {
      return new Value(value.get(0) != 0);
    }

    @Override
    String stringifyValue(ColumnReader reader) {
      return stringifier.stringify(reader.getBoolean());
    }
  }

  private static class DoubleStatValueBuilder extends StatValue.Builder {
    private class Value implements StatValue {
      final double value;

      private Value(double value) {
        this.value = value;
      }

      @Override
      public int compareTo(StatValue o) {
        return comparator.compare(value, ((Value) o).value);
      }

      @Override
      public int compareToValue(ColumnReader reader) {
        return comparator.compare(value, reader.getDouble());
      }

      @Override
      public String toString() {
        return stringifier.stringify(value);
      }
    }

    private DoubleStatValueBuilder(PrimitiveType type) {
      super(type);
    }

    @Override
    StatValue build(ByteBuffer value) {
      return new Value(value.getDouble(0));
    }

    @Override
    String stringifyValue(ColumnReader reader) {
      return stringifier.stringify(reader.getDouble());
    }
  }

  private static class FloatStatValueBuilder extends StatValue.Builder {
    private class Value implements StatValue {
      final float value;

      private Value(float value) {
        this.value = value;
      }

      @Override
      public int compareTo(StatValue o) {
        return comparator.compare(value, ((Value) o).value);
      }

      @Override
      public int compareToValue(ColumnReader reader) {
        return comparator.compare(value, reader.getFloat());
      }

      @Override
      public String toString() {
        return stringifier.stringify(value);
      }
    }

    private FloatStatValueBuilder(PrimitiveType type) {
      super(type);
    }

    @Override
    StatValue build(ByteBuffer value) {
      return new Value(value.getFloat(0));
    }

    @Override
    String stringifyValue(ColumnReader reader) {
      return stringifier.stringify(reader.getFloat());
    }
  }

  private static class IntStatValueBuilder extends StatValue.Builder {
    private class Value implements StatValue {
      final int value;

      private Value(int value) {
        this.value = value;
      }

      @Override
      public int compareTo(StatValue o) {
        return comparator.compare(value, ((Value) o).value);
      }

      @Override
      public int compareToValue(ColumnReader reader) {
        return comparator.compare(value, reader.getInteger());
      }

      @Override
      public String toString() {
        return stringifier.stringify(value);
      }
    }

    private IntStatValueBuilder(PrimitiveType type) {
      super(type);
    }

    @Override
    StatValue build(ByteBuffer value) {
      return new Value(value.getInt(0));
    }

    @Override
    String stringifyValue(ColumnReader reader) {
      return stringifier.stringify(reader.getInteger());
    }
  }

  private static class LongStatValueBuilder extends StatValue.Builder {
    private class Value implements StatValue {
      final long value;

      private Value(long value) {
        this.value = value;
      }

      @Override
      public int compareTo(StatValue o) {
        return comparator.compare(value, ((Value) o).value);
      }

      @Override
      public int compareToValue(ColumnReader reader) {
        return comparator.compare(value, reader.getLong());
      }

      @Override
      public String toString() {
        return stringifier.stringify(value);
      }
    }

    private LongStatValueBuilder(PrimitiveType type) {
      super(type);
    }

    @Override
    StatValue build(ByteBuffer value) {
      return new Value(value.getLong(0));
    }

    @Override
    String stringifyValue(ColumnReader reader) {
      return stringifier.stringify(reader.getLong());
    }
  }

  private static class PageValidator {
    private final int rowGroupNumber;
    private final int columnNumber;
    private final ColumnPath columnPath;
    private final int pageNumber;
    private final int maxDefinitionLevel;
    private final long nullCountInIndex;
    private long nullCountActual;
    private final boolean isNullPage;
    private final ColumnReader columnReader;
    private final List<ContractViolation> violations;
    private final Set<Contract> pageViolations = EnumSet.noneOf(Contract.class);
    private final StatValue minValue;
    private final StatValue maxValue;
    private final StatValue.Builder statValueBuilder;

    PageValidator(
        PrimitiveType type,
        int rowGroupNumber,
        int columnNumber,
        ColumnPath columnPath,
        int pageNumber,
        List<ContractViolation> violations,
        ColumnReader columnReader,
        ByteBuffer minValue,
        ByteBuffer maxValue,
        ByteBuffer prevMinValue,
        ByteBuffer prevMaxValue,
        BoundaryOrder boundaryOrder,
        long nullCount,
        boolean isNullPage) {
      this.columnReader = columnReader;
      this.rowGroupNumber = rowGroupNumber;
      this.columnNumber = columnNumber;
      this.columnPath = columnPath;
      this.pageNumber = pageNumber;
      this.nullCountInIndex = nullCount;
      this.nullCountActual = 0;
      this.isNullPage = isNullPage;
      this.maxDefinitionLevel = columnReader.getDescriptor().getMaxDefinitionLevel();
      this.violations = violations;
      this.statValueBuilder = getBuilder(type);
      this.minValue = isNullPage ? null : statValueBuilder.build(minValue);
      this.maxValue = isNullPage ? null : statValueBuilder.build(maxValue);

      if (isNullPage) {
        // By specification null pages have empty byte arrays as min/max values
        validateContract(!minValue.hasRemaining(),
            NULL_PAGE_HAS_NO_MIN,
            () -> statValueBuilder.build(minValue).toString());
        validateContract(!maxValue.hasRemaining(),
            NULL_PAGE_HAS_NO_MAX,
            () -> statValueBuilder.build(maxValue).toString());
      } else if (prevMinValue != null) {
        validateBoundaryOrder(statValueBuilder.build(prevMinValue), statValueBuilder.build(prevMaxValue),
            boundaryOrder);
      }
    }

    void validateValuesBelongingToRow() {
      do {
        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
          validateValue();
        } else {
          ++nullCountActual;
        }
        columnReader.consume();
      } while (columnReader.getCurrentRepetitionLevel() != 0);
    }

    void finishPage() {
      validateContract(nullCountInIndex == nullCountActual,
          NULL_COUNT_CORRECT,
          () -> Long.toString(nullCountActual),
          () -> Long.toString(nullCountInIndex));
    }

    void validateContract(boolean contractCondition,
        Contract type,
        Supplier<String> value1) {
      validateContract(contractCondition, type, value1, () -> "N/A");
    }

    void validateContract(boolean contractCondition,
        Contract type,
        Supplier<String> value1,
        Supplier<String> value2) {
      if (!contractCondition && !pageViolations.contains(type)) {
        violations.add(
            new ContractViolation(type, value1.get(), value2.get(), rowGroupNumber,
                columnNumber, columnPath, pageNumber));
        pageViolations.add(type);
      }
    }

    private void validateValue() {
      validateContract(!isNullPage,
          NULL_PAGE_HAS_NO_VALUES,
          () -> statValueBuilder.stringifyValue(columnReader));
      validateContract(minValue.compareToValue(columnReader) <= 0,
          MIN_LTEQ_VALUE,
          () -> statValueBuilder.stringifyValue(columnReader),
          minValue::toString);
      validateContract(maxValue.compareToValue(columnReader) >= 0,
          MAX_GTEQ_VALUE,
          () -> statValueBuilder.stringifyValue(columnReader),
          maxValue::toString);
    }

    private void validateBoundaryOrder(StatValue prevMinValue, StatValue prevMaxValue, BoundaryOrder boundaryOrder) {
      switch (boundaryOrder) {
      case ASCENDING:
        validateContract(minValue.compareTo(prevMinValue) >= 0,
            MIN_ASCENDING,
            minValue::toString,
            prevMinValue::toString);
        validateContract(maxValue.compareTo(prevMaxValue) >= 0,
            MAX_ASCENDING,
            maxValue::toString,
            prevMaxValue::toString);
        break;
      case DESCENDING:
        validateContract(minValue.compareTo(prevMinValue) <= 0,
            MIN_DESCENDING,
            minValue::toString,
            prevMinValue::toString);
        validateContract(maxValue.compareTo(prevMaxValue) <= 0,
            MAX_DESCENDING,
            maxValue::toString,
            prevMaxValue::toString);
        break;
      case UNORDERED:
        // No checks necessary.
      }
    }
  }

  public static List<ContractViolation> checkContractViolations(InputFile file) throws IOException {
    List<ContractViolation> violations = new ArrayList<>();
    try (ParquetFileReader reader = ParquetFileReader.open(file)) {
      FileMetaData meta = reader.getFooter().getFileMetaData();
      MessageType schema = meta.getSchema();
      List<ColumnDescriptor> columns = schema.getColumns();

      List<BlockMetaData> blocks = reader.getFooter().getBlocks();
      int rowGroupNumber = 0;
      PageReadStore rowGroup = reader.readNextRowGroup();
      while (rowGroup != null) {
        ColumnReadStore columnReadStore = new ColumnReadStoreImpl(rowGroup,
            new DummyRecordConverter(schema).getRootConverter(), schema, null);
        List<ColumnChunkMetaData> columnChunks = blocks.get(rowGroupNumber).getColumns();
        assert (columnChunks.size() == columns.size());
        for (int columnNumber = 0; columnNumber < columns.size(); ++columnNumber) {
          ColumnDescriptor column = columns.get(columnNumber);
          ColumnChunkMetaData columnChunk = columnChunks.get(columnNumber);
          ColumnIndex columnIndex = reader.readColumnIndex(columnChunk);
          if (columnIndex == null) {
            continue;
          }
          ColumnPath columnPath = columnChunk.getPath();
          OffsetIndex offsetIndex = reader.readOffsetIndex(columnChunk);
          List<ByteBuffer> minValues = columnIndex.getMinValues();
          List<ByteBuffer> maxValues = columnIndex.getMaxValues();
          BoundaryOrder boundaryOrder = columnIndex.getBoundaryOrder();
          List<Long> nullCounts = columnIndex.getNullCounts();
          List<Boolean> nullPages = columnIndex.getNullPages();
          long rowNumber = 0;
          ColumnReader columnReader = columnReadStore.getColumnReader(column);
          ByteBuffer prevMinValue = null;
          ByteBuffer prevMaxValue = null;
          for (int pageNumber = 0; pageNumber < offsetIndex.getPageCount(); ++pageNumber) {
            boolean isNullPage = nullPages.get(pageNumber);
            ByteBuffer minValue = minValues.get(pageNumber);
            ByteBuffer maxValue = maxValues.get(pageNumber);
            PageValidator pageValidator = new PageValidator(
                column.getPrimitiveType(),
                rowGroupNumber, columnNumber, columnPath, pageNumber,
                violations, columnReader,
                minValue,
                maxValue,
                prevMinValue,
                prevMaxValue,
                boundaryOrder,
                nullCounts.get(pageNumber),
                isNullPage);
            if (!isNullPage) {
              prevMinValue = minValue;
              prevMaxValue = maxValue;
            }
            long lastRowNumberInPage = offsetIndex.getLastRowIndex(pageNumber, rowGroup.getRowCount());
            while (rowNumber <= lastRowNumberInPage) {
              pageValidator.validateValuesBelongingToRow();
              ++rowNumber;
            }
            pageValidator.finishPage();
          }
        }
        rowGroup = reader.readNextRowGroup();
        rowGroupNumber++;
      }
    }
    return violations;
  }
}
