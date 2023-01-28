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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for big data applications (such as Apache Spark and Apache Flink).
 * For Intel CPU, Flags containing avx512vbmi and avx512_vbmi2 can have better performance gains.
 */
public class ParquetReadRouter {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetReadRouter.class);

  private static volatile Boolean vector;

  public static void read(int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer) throws IOException {
    if (supportVector()) {
      readBatchVector(bitWidth, in, currentCount, currentBuffer);
    } else {
      readBatch(bitWidth, in, currentCount, currentBuffer);
    }
  }

  public static void readBatchVector(int bitWidth, ByteBufferInputStream in, int currentCount, int[] currentBuffer) throws IOException {
    BytePacker packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    BytePacker packerVector = Packer.LITTLE_ENDIAN.newBytePackerVector(bitWidth);
    int valueIndex = 0;
    int byteIndex = 0;
    int unpackCount = packerVector.getUnpackCount();
    int inputByteCountPerVector = packerVector.getUnpackCount() / 8 * bitWidth;
    int totalByteCount = currentCount * bitWidth / 8;

    // register of avx512 are 512 bits, and can load up to 64 bytes
    int totalByteCountVector = totalByteCount - 64;
    ByteBuffer buffer = in.slice(totalByteCount);
    if (buffer.hasArray()) {
      for (; byteIndex < totalByteCountVector; byteIndex += inputByteCountPerVector, valueIndex += unpackCount) {
        packerVector.unpackValuesVector(buffer.array(), buffer.arrayOffset() + buffer.position() + byteIndex, currentBuffer, valueIndex);
      }
      // If the remaining bytes size <= 64, the remaining bytes are unpacked by packer
      for (; byteIndex < totalByteCount; byteIndex += bitWidth, valueIndex += 8) {
        packer.unpack8Values(buffer.array(), buffer.arrayOffset() + buffer.position() + byteIndex, currentBuffer, valueIndex);
      }
    } else {
      for (; byteIndex < totalByteCountVector; byteIndex += inputByteCountPerVector, valueIndex += unpackCount) {
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

  public static Boolean supportVector() {
    if (vector != null) {
      return vector;
    }
    synchronized (ParquetReadRouter.class) {
      if (vector == null) {
        synchronized (ParquetReadRouter.class) {
          vector = avx512Flag();
        }
      }
    }
    return vector;
  }

  private static boolean avx512Flag() {
    try {
      String os = System.getProperty("os.name");
      if (os == null || !os.toLowerCase().startsWith("linux")) {
        return false;
      }
      List<String> allLines = Files.readAllLines(Paths.get("/proc/cpuinfo"), StandardCharsets.UTF_8);
      for (String line : allLines) {
        if (line != null && line.startsWith("flags")) {
          int index = line.indexOf(":");
          if (index < 0) {
            continue;
          }
          line = line.substring(index + 1);
          Set<String> flagsSet = Arrays.stream(line.split(" ")).collect(Collectors.toSet());
          if (flagsSet.contains("avx512vbmi") && flagsSet.contains("avx512_vbmi2")) {
            return true;
          }
        }
      }
    } catch (Exception ex) {
      LOG.warn("Failed to gett CPU info");
    }
    return false;
  }
}
