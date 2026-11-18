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
import java.nio.ByteOrder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Column writer for variable-width types (STRING, BINARY) using PLAIN encoding.
 *
 * <p>Arrow stores variable-width data as an offset buffer (int32[N+1]) plus a contiguous data
 * buffer. Parquet PLAIN encoding stores each value as [4-byte length][value bytes]. This writer
 * transforms between the two layouts in a single pass.
 *
 * <p>Handles both nullable and non-null columns via the Arrow validity bitmap.
 */
class VarWidthPlainWriter implements ArrowColumnWriter {

  private final PageWriter pageWriter;
  private final PrimitiveType type;
  private final int maxDefinitionLevel;
  private final boolean isNullable;

  VarWidthPlainWriter(PageWriter pageWriter, PrimitiveType type, int maxDefinitionLevel,
      boolean isNullable) {
    this.pageWriter = pageWriter;
    this.type = type;
    this.maxDefinitionLevel = maxDefinitionLevel;
    this.isNullable = isNullable;
  }

  @Override
  public void write(FieldVector vector, int offset, int length) throws IOException {
    BaseVariableWidthVector varVector = (BaseVariableWidthVector) vector;
    ArrowBuf offsetBuf = varVector.getOffsetBuffer();
    ArrowBuf dataBuf = varVector.getDataBuffer();
    ArrowBuf validityBuf = vector.getValidityBuffer();

    // Calculate total data size for non-null values
    int nullCount = 0;
    int totalDataBytes = 0;
    for (int i = 0; i < length; i++) {
      if (isNullable && isNull(validityBuf, offset + i)) {
        nullCount++;
      } else {
        int start = offsetBuf.getInt((long) (offset + i) * Integer.BYTES);
        int end = offsetBuf.getInt((long) (offset + i + 1) * Integer.BYTES);
        totalDataBytes += (end - start);
      }
    }

    int nonNullCount = length - nullCount;
    // Parquet PLAIN for binary: [4-byte length][data] per value
    int pageDataSize = nonNullCount * Integer.BYTES + totalDataBytes;
    ByteBuffer pageData = ByteBuffer.allocate(pageDataSize);
    pageData.order(ByteOrder.LITTLE_ENDIAN);

    // Build statistics while writing
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(type);
    stats.setNumNulls(nullCount);

    for (int i = 0; i < length; i++) {
      if (isNullable && isNull(validityBuf, offset + i)) {
        continue; // skip null values in data section
      }
      int start = offsetBuf.getInt((long) (offset + i) * Integer.BYTES);
      int end = offsetBuf.getInt((long) (offset + i + 1) * Integer.BYTES);
      int len = end - start;

      // Write length-prefixed value
      pageData.putInt(len);
      if (len > 0) {
        byte[] valueBytes = new byte[len];
        dataBuf.getBytes(start, valueBytes);
        pageData.put(valueBytes);
        stats.updateStats(Binary.fromReusedByteArray(valueBytes));
      } else {
        stats.updateStats(Binary.EMPTY);
      }
    }
    pageData.flip();

    // Repetition levels: all 0 (flat schema)
    BytesInput rl = LevelEncoder.encodeConstant(0, length, 0);

    // Definition levels
    BytesInput dl;
    if (isNullable) {
      dl = LevelEncoder.encodeFromValidityBitmap(validityBuf, offset, length, maxDefinitionLevel);
    } else {
      dl = LevelEncoder.encodeConstant(maxDefinitionLevel, length, maxDefinitionLevel);
    }

    pageWriter.writePage(
        BytesInput.concat(rl, dl, BytesInput.from(pageData)),
        length,
        length,
        stats,
        Encoding.RLE,
        Encoding.RLE,
        Encoding.PLAIN);
  }

  private static boolean isNull(ArrowBuf validityBuf, int index) {
    if (validityBuf == null || validityBuf.capacity() == 0) {
      return false;
    }
    int byteIndex = index >> 3;
    int bitIndex = index & 7;
    return ((validityBuf.getByte(byteIndex) >> bitIndex) & 1) == 0;
  }
}
