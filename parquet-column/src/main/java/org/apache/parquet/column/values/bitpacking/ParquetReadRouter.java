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
package org.apache.parquet.column.values.bitpacking;

import org.apache.parquet.bytes.ByteBufferInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is provided for big data applications(such as Spark)
 *
 * - For Intel CPU, Flags avx512_vbmi2 && avx512_vbmi2 can have better performance gains
 */

public class ParquetReadRouter {
  public static void readBatchVector(int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer) throws IOException {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    BytePacker packerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);
    int valueIndex = 0;
    int byteIndex = 0;
    int outCountPerVector = packerVector.getUnpackCount();
    int inputByteCountPerVector = packerVector.getUnpackCount() / 8 * bitWidth;
    int totalByteCount = currentCount * bitWidth / 8;
    int totalByteCountVector = totalByteCount - inputByteCountPerVector * 2;
    ByteBuffer buffer = in.slice(totalByteCount);
    if (buffer.hasArray()) {
      for (; byteIndex < totalByteCountVector; byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
        packerVector.unpackValuesVector(buffer.array(), buffer.arrayOffset() + buffer.position() + byteIndex, currentBuffer, valueIndex);
      }
      for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
        packer.unpack8Values(buffer.array(), buffer.arrayOffset() + buffer.position() + byteIndex, currentBuffer, valueIndex);
      }
    } else {
      for (; byteIndex < totalByteCountVector; byteIndex += inputByteCountPerVector, valueIndex += outCountPerVector) {
        packerVector.unpackValuesVector(buffer, buffer.position() + byteIndex, currentBuffer, valueIndex);
      }
      for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
        packer.unpack8Values(buffer, buffer.position() + byteIndex, currentBuffer, valueIndex);
      }
    }
  }
  public static void readBatch(int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer) throws EOFException {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    int valueIndex = 0;
    while (valueIndex < currentCount) {
      // values are bit packed 8 at a time, so reading bitWidth will always work
      ByteBuffer buffer = in.slice(bitWidth);
      packer.unpack8Values(buffer, buffer.position(), currentBuffer, valueIndex);
      valueIndex += 8;
    }
  }
}
