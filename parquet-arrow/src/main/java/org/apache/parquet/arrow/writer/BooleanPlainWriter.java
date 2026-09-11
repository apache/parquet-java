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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Column writer for BOOLEAN columns using PLAIN encoding.
 *
 * <p>Both Arrow and Parquet store booleans as bit-packed values (1 bit per value).
 * Arrow uses LSB bit ordering within each byte, which matches Parquet's boolean
 * PLAIN encoding. For non-null columns, the data buffer can be used directly.
 * For nullable columns, non-null values are compacted.
 */
class BooleanPlainWriter implements ArrowColumnWriter {

  private final PageWriter pageWriter;
  private final PrimitiveType type;
  private final int maxDefinitionLevel;
  private final boolean isNullable;

  BooleanPlainWriter(PageWriter pageWriter, PrimitiveType type, int maxDefinitionLevel,
      boolean isNullable) {
    this.pageWriter = pageWriter;
    this.type = type;
    this.maxDefinitionLevel = maxDefinitionLevel;
    this.isNullable = isNullable;
  }

  @Override
  public void write(FieldVector vector, int offset, int length) throws IOException {
    BitVector bitVector = (BitVector) vector;
    ArrowBuf validityBuf = vector.getValidityBuffer();
    ArrowBuf dataBuf = bitVector.getDataBuffer();

    int nullCount = 0;
    int trueCount = 0;

    if (isNullable) {
      for (int i = 0; i < length; i++) {
        if (isNull(validityBuf, offset + i)) {
          nullCount++;
        } else if (getBit(dataBuf, offset + i)) {
          trueCount++;
        }
      }
    } else {
      for (int i = 0; i < length; i++) {
        if (getBit(dataBuf, offset + i)) {
          trueCount++;
        }
      }
    }

    int nonNullCount = length - nullCount;

    // Produce bit-packed boolean data for non-null values
    int byteCount = (nonNullCount + 7) / 8;
    byte[] booleanBytes = new byte[byteCount];
    int bitPos = 0;

    for (int i = 0; i < length; i++) {
      if (isNullable && isNull(validityBuf, offset + i)) {
        continue;
      }
      if (getBit(dataBuf, offset + i)) {
        booleanBytes[bitPos / 8] |= (1 << (bitPos % 8));
      }
      bitPos++;
    }

    // Statistics
    BooleanStatistics stats = (BooleanStatistics) Statistics.createStats(type);
    stats.setNumNulls(nullCount);
    if (nonNullCount > 0) {
      boolean hasTrue = trueCount > 0;
      boolean hasFalse = (nonNullCount - trueCount) > 0;
      if (hasTrue && hasFalse) {
        stats.updateStats(true);
        stats.updateStats(false);
      } else if (hasTrue) {
        stats.updateStats(true);
      } else {
        stats.updateStats(false);
      }
    }

    // Repetition levels
    BytesInput rl = LevelEncoder.encodeConstant(0, length, 0);

    // Definition levels
    BytesInput dl;
    if (isNullable) {
      dl = LevelEncoder.encodeFromValidityBitmap(validityBuf, offset, length, maxDefinitionLevel);
    } else {
      dl = LevelEncoder.encodeConstant(maxDefinitionLevel, length, maxDefinitionLevel);
    }

    pageWriter.writePage(
        BytesInput.concat(rl, dl, BytesInput.from(booleanBytes)),
        length,
        length,
        stats,
        Encoding.RLE,
        Encoding.RLE,
        Encoding.PLAIN);
  }

  private static boolean getBit(ArrowBuf buf, int index) {
    int byteIndex = index >> 3;
    int bitIndex = index & 7;
    return ((buf.getByte(byteIndex) >> bitIndex) & 1) == 1;
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
