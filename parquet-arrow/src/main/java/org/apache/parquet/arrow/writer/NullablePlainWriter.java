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
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Column writer for nullable, fixed-width, PLAIN-encoded columns.
 *
 * <p>Single-pass: iterates values once, copying non-null values to a compacted buffer,
 * computing statistics, and counting nulls simultaneously.
 *
 * <p>Conditions: PLAIN encoding, column is optional (nullable), type is fixed-width.
 */
class NullablePlainWriter implements ArrowColumnWriter {

  private final PageWriter pageWriter;
  private final PrimitiveType type;
  private final int typeWidthBytes;
  private final int maxDefinitionLevel;

  NullablePlainWriter(PageWriter pageWriter, PrimitiveType type, int typeWidthBytes,
      int maxDefinitionLevel) {
    this.pageWriter = pageWriter;
    this.type = type;
    this.typeWidthBytes = typeWidthBytes;
    this.maxDefinitionLevel = maxDefinitionLevel;
  }

  @Override
  public void write(FieldVector vector, int offset, int length) throws IOException {
    ArrowBuf dataBuf = vector.getDataBuffer();
    ArrowBuf validityBuf = vector.getValidityBuffer();

    // Allocate output at max possible size (all non-null). Single pass fills it.
    ByteBuffer compactedData = ByteBuffer.allocate(length * typeWidthBytes);
    compactedData.order(ByteOrder.LITTLE_ENDIAN);

    ByteBuffer srcView = dataBuf.nioBuffer(
        (long) offset * typeWidthBytes, (int) ((long) length * typeWidthBytes));
    srcView.order(ByteOrder.LITTLE_ENDIAN);

    // Single-pass: compact non-null values + compute stats + count nulls
    Statistics<?> stats = Statistics.createStats(type);
    int nullCount = 0;

    for (int i = 0; i < length; i++) {
      if (isNull(validityBuf, offset + i)) {
        nullCount++;
      } else {
        int srcPos = i * typeWidthBytes;
        // Copy value bytes
        for (int b = 0; b < typeWidthBytes; b++) {
          compactedData.put(srcView.get(srcPos + b));
        }
        // Update stats from the source buffer
        updateStats(stats, srcView, srcPos);
      }
    }

    stats.setNumNulls(nullCount);
    compactedData.flip();

    // Repetition levels: all 0 (flat schema)
    BytesInput rl = LevelEncoder.encodeConstant(0, length, 0);

    // Definition levels: from validity bitmap
    BytesInput dl = LevelEncoder.encodeFromValidityBitmap(
        validityBuf, offset, length, maxDefinitionLevel);

    // Write page
    pageWriter.writePage(
        BytesInput.concat(rl, dl, BytesInput.from(compactedData)),
        length,
        length,
        stats,
        Encoding.RLE,
        Encoding.RLE,
        Encoding.PLAIN);
  }

  private void updateStats(Statistics<?> stats, ByteBuffer buf, int pos) {
    switch (type.getPrimitiveTypeName()) {
      case INT32:
        stats.updateStats(buf.getInt(pos));
        break;
      case INT64:
        stats.updateStats(buf.getLong(pos));
        break;
      case FLOAT:
        stats.updateStats(buf.getFloat(pos));
        break;
      case DOUBLE:
        stats.updateStats(buf.getDouble(pos));
        break;
      default:
        // For other fixed-width types, skip stats (still produces valid file)
        break;
    }
  }

  private static boolean isNull(ArrowBuf validityBuf, int index) {
    if (validityBuf == null || validityBuf.capacity() == 0) {
      return false; // No validity buffer means all values are non-null
    }
    int byteIndex = index >> 3;
    int bitIndex = index & 7;
    return ((validityBuf.getByte(byteIndex) >> bitIndex) & 1) == 0;
  }
}
