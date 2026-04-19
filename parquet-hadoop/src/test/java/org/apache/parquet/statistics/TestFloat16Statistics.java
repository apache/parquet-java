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
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Float16;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFloat16Statistics {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Binary[] valuesInAscendingOrder = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0}), // -2.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // -6.109476E-5
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesInAscendingOrderMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesInDescendingOrder = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c}), // Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // -6.109476E-5
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0}), // -2.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc})
  }; // -Infinity

  private Binary[] valuesInDescendingOrderMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesUndefinedOrder = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c}), // Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0}), // -2.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // -6.109476E-5
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc})
  }; // -Infinity

  private Binary[] valuesUndefinedOrderMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesAllPositiveZero = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00})
  }; // +0

  // Float16Builder: Updating min to -0.0 to ensure that no 0.0 values would be skipped
  private Binary[] valuesAllPositiveStatsZeroMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00})
  }; // +0

  private Binary[] valuesAllNegativeZero = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80})
  }; // -0

  // Float16Builder: Updating max to +0.0 to ensure that no 0.0 values would be skipped
  private Binary[] valuesAllNegativeStatsZeroMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00})
  }; // +0

  private Binary[] valuesWithNaN = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0}), // -2.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7e})
  }; // NaN

  // Float16Builder: Drop min/max values in case of NaN as the sorting order of values is undefined
  private Binary[] valuesWithNaNStatsMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00})
  }; // +0

  @Test
  public void testFloat16StatisticsMultipleCases() throws IOException {
    List<Binary[]> testValues = List.of(
        valuesInAscendingOrder,
        valuesInDescendingOrder,
        valuesUndefinedOrder,
        valuesAllPositiveZero,
        valuesAllNegativeZero,
        valuesWithNaN);
    List<Binary[]> expectedValues = List.of(
        valuesInAscendingOrderMinMax,
        valuesInDescendingOrderMinMax,
        valuesUndefinedOrderMinMax,
        valuesAllPositiveStatsZeroMinMax,
        valuesAllNegativeStatsZeroMinMax,
        valuesWithNaNStatsMinMax);

    for (int i = 0; i < testValues.size(); ++i) {
      MessageType schema = Types.buildMessage()
          .required(FIXED_LEN_BYTE_ARRAY)
          .as(float16Type())
          .length(2)
          .named("col_float16")
          .named("msg");

      Configuration conf = new Configuration();
      GroupWriteSupport.setSchema(schema, conf);

      GroupFactory factory = new SimpleGroupFactory(schema);
      Path path = newTempPath();
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
          .withConf(conf)
          .withDictionaryEncoding(false)
          .build()) {

        for (Binary value : testValues.get(i)) {
          writer.write(factory.newGroup().append("col_float16", value));
        }
      }

      try (ParquetFileReader reader =
          ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
        ColumnChunkMetaData column =
            reader.getFooter().getBlocks().get(0).getColumns().get(0);
        Statistics<?> statistics = column.getStatistics();

        assertArrayEquals(expectedValues.get(i)[0].getBytes(), statistics.getMinBytes());
        assertArrayEquals(expectedValues.get(i)[1].getBytes(), statistics.getMaxBytes());
      }
    }
  }

  @Test
  public void testFloat16Statistics() throws IOException {
    for (int i = 0; i < valuesInAscendingOrder.length; ++i) {
      for (int j = 0; j < valuesInAscendingOrder.length; ++j) {
        int minIndex = i;
        int maxIndex = j;

        if (Float16.compare(
                valuesInAscendingOrder[i].get2BytesLittleEndian(),
                valuesInAscendingOrder[j].get2BytesLittleEndian())
            > 0) {
          minIndex = j;
          maxIndex = i;
        }

        // Refer to Float16Builder class
        if (valuesInAscendingOrder[minIndex].get2BytesLittleEndian() == (short) 0x0000) {
          minIndex = 3;
        }
        if (valuesInAscendingOrder[maxIndex].get2BytesLittleEndian() == (short) 0x8000) {
          maxIndex = 4;
        }

        MessageType schema = Types.buildMessage()
            .required(FIXED_LEN_BYTE_ARRAY)
            .as(float16Type())
            .length(2)
            .named("col_float16")
            .named("msg");

        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(schema, conf);

        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = newTempPath();
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
            .withConf(conf)
            .withDictionaryEncoding(false)
            .build()) {
          writer.write(factory.newGroup().append("col_float16", valuesInAscendingOrder[i]));
          writer.write(factory.newGroup().append("col_float16", valuesInAscendingOrder[j]));
        }

        try (ParquetFileReader reader =
            ParquetFileReader.open(HadoopInputFile.fromPath(path, new Configuration()))) {
          ColumnChunkMetaData column =
              reader.getFooter().getBlocks().get(0).getColumns().get(0);
          Statistics<?> statistics = column.getStatistics();

          assertArrayEquals(valuesInAscendingOrder[minIndex].getBytes(), statistics.getMinBytes());
          assertArrayEquals(valuesInAscendingOrder[maxIndex].getBytes(), statistics.getMaxBytes());
        }
      }
    }
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }
}
