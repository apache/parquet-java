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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestInterOpReadFloat16 {
  private static String FLOAT16_NONZEROS_NANS_FILE = "float16_nonzeros_and_nans.parquet";
  private static String FLOAT16_ZEROS_NANS_FILE = "float16_zeros_and_nans.parquet";
  private static final String CHANGESET = "da467da";

  private InterOpTester interop = new InterOpTester();

  @Test
  public void testInterOpReadFloat16NonZerosAndNansParquetFiles() throws IOException {
    Path filePath = interop.GetInterOpFile(FLOAT16_NONZEROS_NANS_FILE, CHANGESET);

    final int expectRows = 8;
    Binary[] c0ExpectValues = {
      null,
      Binary.fromConstantByteArray(new byte[] {0x00, 0x3c}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xc0}),
      Binary.fromConstantByteArray(new byte[] {0x00, 0x7e}),
      Binary.fromConstantByteArray(new byte[] {0x00, 0x00}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0xbc}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x80}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x40}),
    };

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(filePath, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);

      assertArrayEquals(
          new byte[] {0x00, (byte) 0xc0}, column.getStatistics().getMinBytes());
      // 0x40 equals @ in ASCII
      assertArrayEquals(new byte[] {0x00, 0x40}, column.getStatistics().getMaxBytes());
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        if (group == null) {
          fail("Should not reach end of file before " + expectRows + " rows");
        }
        if (group.getFieldRepetitionCount(0) != 0) {
          // Check if the field is not null
          assertEquals(c0ExpectValues[i], group.getBinary(0, 0));
        } else {
          // Check if the field is null
          assertEquals(0, i);
        }
      }
    }
  }

  @Test
  public void testInterOpReadFloat16ZerosAndNansParquetFiles() throws IOException {
    Path filePath = interop.GetInterOpFile(FLOAT16_ZEROS_NANS_FILE, "da467da");

    final int expectRows = 3;
    Binary[] c0ExpectValues = {
      null,
      Binary.fromConstantByteArray(new byte[] {0x00, 0x00}),
      Binary.fromConstantByteArray(new byte[] {0x00, (byte) 0x7e})
    };

    try (ParquetFileReader reader =
        ParquetFileReader.open(HadoopInputFile.fromPath(filePath, new Configuration()))) {
      ColumnChunkMetaData column =
          reader.getFooter().getBlocks().get(0).getColumns().get(0);

      assertArrayEquals(
          new byte[] {0x00, (byte) 0x80}, column.getStatistics().getMinBytes());
      assertArrayEquals(new byte[] {0x00, 0x00}, column.getStatistics().getMaxBytes());
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), filePath).build()) {
      for (int i = 0; i < expectRows; ++i) {
        Group group = reader.read();
        if (group == null) {
          fail("Should not reach end of file before " + expectRows + " rows");
        }
        if (group.getFieldRepetitionCount(0) != 0) {
          // Check if the field is not null
          assertEquals(c0ExpectValues[i], group.getBinary(0, 0));
        } else {
          // Check if the field is null
          assertEquals(0, i);
        }
      }
    }
  }
}
