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

public class ByteStreamSplitValuesReaderForFloat extends ByteStreamSplitValuesReader {

  private byte[] valueByteBuffer;

  public ByteStreamSplitValuesReaderForFloat() {
    super(Float.BYTES);
    valueByteBuffer = new byte[Float.BYTES];
  }

  @Override
  public float readFloat() {
    try {
      gatherElementDataFromStreams(valueByteBuffer);
      int value = (int)(valueByteBuffer[0] & 0xFF) |
                  ((int)(valueByteBuffer[1] & 0xFF) << 8) |
                  ((int)(valueByteBuffer[2] & 0xFF) << 16) |
                  ((int)(valueByteBuffer[3] & 0xFF) << 24);
      return Float.intBitsToFloat(value);
    } catch (IOException | ArrayIndexOutOfBoundsException ex) {
      throw new ParquetDecodingException("Could not read float.", ex);
    }
  }
}
