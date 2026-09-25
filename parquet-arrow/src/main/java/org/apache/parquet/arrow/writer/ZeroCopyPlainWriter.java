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
package org.apache.parquet.arrow.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Zero-copy column writer for non-null, fixed-width, PLAIN-encoded columns.
 *
 * <p>Arrow's data buffer for fixed-width types (Int32, Int64, Float, Double) is stored as
 * contiguous little-endian values — which is identical to Parquet's PLAIN encoding. This writer
 * wraps the Arrow buffer directly as a Parquet page without copying or transforming any bytes.
 *
 * <p>Conditions: PLAIN encoding, column is required (non-null), type is fixed-width.
 */
class ZeroCopyPlainWriter implements ArrowColumnWriter {

  private final PageWriter pageWriter;
  private final PrimitiveType type;
  private final int typeWidthBytes;
  private final int maxDefinitionLevel;

  /**
   * @param pageWriter the Parquet page writer for this column
   * @param type the Parquet primitive type descriptor
   * @param typeWidthBytes the byte width of each value (4 for INT32, 8 for INT64/FLOAT64, etc.)
   * @param maxDefinitionLevel the max definition level for this column
   */
  ZeroCopyPlainWriter(PageWriter pageWriter, PrimitiveType type, int typeWidthBytes,
      int maxDefinitionLevel) {
    this.pageWriter = pageWriter;
    this.type = type;
    this.typeWidthBytes = typeWidthBytes;
    this.maxDefinitionLevel = maxDefinitionLevel;
  }

  @Override
  public void write(FieldVector vector, int offset, int length) throws IOException {
    ArrowBuf dataBuf = vector.getDataBuffer();

    // Data: wrap Arrow's data buffer directly — zero copy
    long startByte = (long) offset * typeWidthBytes;
    long lengthBytes = (long) length * typeWidthBytes;
    ByteBuffer nioBuffer = dataBuf.nioBuffer(startByte, (int) lengthBytes);
    BytesInput data = BytesInput.from(nioBuffer);

    // Repetition levels: all 0 for flat schema — encode as single RLE run
    BytesInput rl = LevelEncoder.encodeConstant(0, length, 0);

    // Definition levels: all max for non-null — encode as single RLE run
    BytesInput dl = LevelEncoder.encodeConstant(maxDefinitionLevel, length, maxDefinitionLevel);

    // Statistics: sequential scan of the buffer
    StatsComputer.StatsResult statsResult = StatsComputer.compute(dataBuf, offset, length, type);

    // Write the assembled page
    pageWriter.writePage(
        BytesInput.concat(rl, dl, data),
        length, // valueCount
        length, // rowCount (same for flat schema)
        statsResult.statistics,
        Encoding.RLE, // RL encoding
        Encoding.RLE, // DL encoding
        Encoding.PLAIN); // values encoding
  }
}
