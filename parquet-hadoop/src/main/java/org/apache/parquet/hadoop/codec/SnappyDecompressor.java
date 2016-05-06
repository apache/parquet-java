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
package org.apache.parquet.hadoop.codec;

import org.xerial.snappy.Snappy;
import java.io.IOException;
import java.nio.ByteBuffer;

public class SnappyDecompressor extends NonBlockingDecompressor {
  private int outputBufferSize = -1;

  @Override
  public void setOutputBufferSize(int size) {
    this.outputBufferSize = size;
  }

  @Override
  protected int getUncompressedLength(ByteBuffer inputBuffer) throws IOException {
    return outputBufferSize < 0 ? Snappy.uncompressedLength(inputBuffer) : outputBufferSize;
  }

  @Override
  protected int decompress(ByteBuffer inputBuffer, ByteBuffer outputBuffer) throws IOException {
    return Snappy.uncompress(inputBuffer, outputBuffer);
  }

} //class SnappyDecompressor
