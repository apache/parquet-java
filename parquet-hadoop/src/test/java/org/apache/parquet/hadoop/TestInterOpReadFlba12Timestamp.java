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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

public class TestInterOpReadFlba12Timestamp {
  private static final String FILE = "flba12_timestamp.parquet";
  // TODO: update after parquet-testing PR merges
  private static final String CHANGESET = "PLACEHOLDER";

  // The six timestamps stored in the file, in row order.
  private static final String[] EXPECTED_TIMESTAMPS = {
    "1970-01-01T00:00:00", // row 0: epoch
    "1970-01-01T00:00:01", // row 1: +1 s
    "1969-12-31T23:59:59", // row 2: -1 s (pre-1970, negative)
    "2262-04-11T23:47:16", // row 3: near the INT64-nanos max boundary
    "9999-12-31T23:59:59", // row 4: far future (NANOS needs > 64 bits)
    "0001-01-01T00:00:00", // row 5: far past (NANOS is < -2^63)
  };
  // Row indices of the minimum (year 0001) and maximum (year 9999) timestamps.
  private static final int MIN_ROW = 5;
  private static final int MAX_ROW = 4;

  // Fractional-second digit counts the UTC PrimitiveStringifier renders per unit.
  private static final int MILLIS_FRACTION_DIGITS = 3;
  private static final int MICROS_FRACTION_DIGITS = 6;
  private static final int NANOS_FRACTION_DIGITS = 9;

  private final InterOpTester interop = new InterOpTester();

  // Stringify the expected timestamp value for the given row with the required fractional digits.
  private static String expected(int row, int fractionDigits) {
    StringBuilder sb = new StringBuilder(EXPECTED_TIMESTAMPS[row]).append('.');
    for (int i = 0; i < fractionDigits; i++) {
      sb.append('0');
    }
    return sb.append("+0000").toString();
  }

  @Test
  public void testInterOpReadFlba12TimestampParquetFile() throws IOException {
    Path filePath = interop.GetInterOpFile(FILE, CHANGESET);

    final int expectRows = EXPECTED_TIMESTAMPS.length;

    PrimitiveStringifier millisStringifier = null;
    PrimitiveStringifier microsStringifier = null;
    PrimitiveStringifier nanosStringifier = null;

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(filePath, new Configuration()))) {
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      BlockMetaData block = reader.getFooter().getBlocks().get(0);

      // timestamp_millis column
      PrimitiveType millisType = schema.getType("timestamp_millis").asPrimitiveType();
      assertThat(millisType.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
      assertThat(millisType.getTypeLength()).isEqualTo(12);
      assertThat(millisType.getLogicalTypeAnnotation())
          .isEqualTo(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS));
      assertThat(millisType.columnOrder()).isEqualTo(ColumnOrder.typeDefined());
      millisStringifier = millisType.stringifier();

      // timestamp_micros column
      PrimitiveType microsType = schema.getType("timestamp_micros").asPrimitiveType();
      assertThat(microsType.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
      assertThat(microsType.getTypeLength()).isEqualTo(12);
      assertThat(microsType.getLogicalTypeAnnotation())
          .isEqualTo(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS));
      assertThat(microsType.columnOrder()).isEqualTo(ColumnOrder.typeDefined());
      microsStringifier = microsType.stringifier();

      // timestamp_nanos column
      PrimitiveType nanosType = schema.getType("timestamp_nanos").asPrimitiveType();
      assertThat(nanosType.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
      assertThat(nanosType.getTypeLength()).isEqualTo(12);
      assertThat(nanosType.getLogicalTypeAnnotation())
          .isEqualTo(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS));
      assertThat(nanosType.columnOrder()).isEqualTo(ColumnOrder.typeDefined());
      nanosStringifier = nanosType.stringifier();

      // Statistics: min = year 0001, max = year 9999.
      ColumnChunkMetaData millisCol = block.getColumns().get(0);
      assertThat(millisStringifier.stringify(Binary.fromConstantByteArray(
              millisCol.getStatistics().getMinBytes())))
          .isEqualTo(expected(MIN_ROW, MILLIS_FRACTION_DIGITS));
      assertThat(millisStringifier.stringify(Binary.fromConstantByteArray(
              millisCol.getStatistics().getMaxBytes())))
          .isEqualTo(expected(MAX_ROW, MILLIS_FRACTION_DIGITS));

      ColumnChunkMetaData microsCol = block.getColumns().get(1);
      assertThat(microsStringifier.stringify(Binary.fromConstantByteArray(
              microsCol.getStatistics().getMinBytes())))
          .isEqualTo(expected(MIN_ROW, MICROS_FRACTION_DIGITS));
      assertThat(microsStringifier.stringify(Binary.fromConstantByteArray(
              microsCol.getStatistics().getMaxBytes())))
          .isEqualTo(expected(MAX_ROW, MICROS_FRACTION_DIGITS));

      ColumnChunkMetaData nanosCol = block.getColumns().get(2);
      assertThat(nanosStringifier.stringify(Binary.fromConstantByteArray(
              nanosCol.getStatistics().getMinBytes())))
          .isEqualTo(expected(MIN_ROW, NANOS_FRACTION_DIGITS));
      assertThat(nanosStringifier.stringify(Binary.fromConstantByteArray(
              nanosCol.getStatistics().getMaxBytes())))
          .isEqualTo(expected(MAX_ROW, NANOS_FRACTION_DIGITS));
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        assertThat(group)
            .as("Should not reach end of file before " + expectRows + " rows")
            .isNotNull();
        assertThat(group.getFieldRepetitionCount(0))
            .as("timestamp_millis should not be null at row " + i)
            .isNotEqualTo(0);
        assertThat(millisStringifier.stringify(group.getBinary(0, 0)))
            .isEqualTo(expected(i, MILLIS_FRACTION_DIGITS));
        assertThat(group.getFieldRepetitionCount(1))
            .as("timestamp_micros should not be null at row " + i)
            .isNotEqualTo(0);
        assertThat(microsStringifier.stringify(group.getBinary(1, 0)))
            .isEqualTo(expected(i, MICROS_FRACTION_DIGITS));
        assertThat(group.getFieldRepetitionCount(2))
            .as("timestamp_nanos should not be null at row " + i)
            .isNotEqualTo(0);
        assertThat(nanosStringifier.stringify(group.getBinary(2, 0)))
            .isEqualTo(expected(i, NANOS_FRACTION_DIGITS));
      }
    }
  }
}
