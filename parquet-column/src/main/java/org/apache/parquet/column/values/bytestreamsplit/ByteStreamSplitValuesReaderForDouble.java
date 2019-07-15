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
package org.apache.parquet.column.values.bytestreamsplit;

import org.apache.parquet.io.ParquetDecodingException;

import java.io.IOException;

public class ByteStreamSplitValuesReaderForDouble extends ByteStreamSplitValuesReader {

  private byte[] valueByteBuffer;

  public ByteStreamSplitValuesReaderForDouble() {
    super(Double.BYTES);
    valueByteBuffer = new byte[Double.BYTES];
  }

  @Override
  public double readDouble() {
    try {
      gatherElementDataFromStreams(valueByteBuffer);
      long value = (long)(valueByteBuffer[0] & 0xFF) |
                   ((long)(valueByteBuffer[1] & 0xFF) << 8) |
                   ((long)(valueByteBuffer[2] & 0xFF) << 16) |
                   ((long)(valueByteBuffer[3] & 0xFF) << 24) |
                   ((long)(valueByteBuffer[4] & 0xFF) << 32) |
                   ((long)(valueByteBuffer[5] & 0xFF) << 40) |
                   ((long)(valueByteBuffer[6] & 0xFF) << 48) |
                   ((long)(valueByteBuffer[7] & 0xFF) << 56);
      return Double.longBitsToDouble(value);
    } catch (IOException | ArrayIndexOutOfBoundsException ex) {
      throw new ParquetDecodingException("Could not read double.", ex);
    }
  }
}
