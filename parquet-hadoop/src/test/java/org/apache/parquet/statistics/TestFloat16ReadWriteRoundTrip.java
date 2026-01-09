/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.statistics;

import static org.apache.parquet.schema.LogicalTypeAnnotation.float16Type;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFloat16ReadWriteRoundTrip {

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

  private Binary[] valuesAllPositiveZero = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00})
  }; // +0

  private Binary[] valuesAllNegativeZero = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80})
  }; // -0

  private Binary[] valuesWithNaN = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0}), // -2.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0xff, (byte) 0x7b}), // 65504.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x01, (byte) 0x00}), // 5.9604645E-8
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7e})
  }; // NaN

  private Binary[] valuesInAscendingOrderMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesInDescendingOrderMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesUndefinedOrderMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xfc}), // -Infinity
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7c})
  }; // Infinity

  private Binary[] valuesAllPositiveZeroMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00}), // +0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x00})
  }; // +0

  private Binary[] valuesAllNegativeZeroMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80}), // -0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x80})
  }; // -0

  private Binary[] valuesWithNaNMinMax = {
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0xc0}), // -2.0
    Binary.fromConstantByteArray(new byte[] {(byte) 0x00, (byte) 0x7e})
  }; // NaN

  @Test
  public void testFloat16ColumnIndex() throws IOException {
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
        valuesAllPositiveZeroMinMax,
        valuesAllNegativeZeroMinMax,
        valuesWithNaNMinMax);

    for (int i = 0; i < testValues.size(); i++) {
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
        ColumnIndex index = reader.readColumnIndex(column);
        assertEquals(Collections.singletonList(expectedValues.get(i)[0]), toFloat16List(index.getMinValues()));
        assertEquals(Collections.singletonList(expectedValues.get(i)[1]), toFloat16List(index.getMaxValues()));
      }
    }
  }

  private Path newTempPath() throws IOException {
    File file = temp.newFile();
    Preconditions.checkArgument(file.delete(), "Could not remove temp file");
    return new Path(file.getAbsolutePath());
  }

  private static List<Binary> toFloat16List(List<ByteBuffer> buffers) {
    return buffers.stream()
        .map(buffer -> Binary.fromConstantByteArray(buffer.array()))
        .collect(Collectors.toList());
  }
}
