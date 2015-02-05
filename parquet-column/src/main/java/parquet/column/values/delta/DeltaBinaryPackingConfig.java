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
package parquet.column.values.delta;


import parquet.Preconditions;
import parquet.bytes.BytesInput;
import parquet.bytes.BytesUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * Config for delta binary packing
 *
 * @author Tianshuo Deng
 */
class DeltaBinaryPackingConfig {
  final int blockSizeInValues;
  final int miniBlockNumInABlock;
  final int miniBlockSizeInValues;

  public DeltaBinaryPackingConfig(int blockSizeInValues, int miniBlockNumInABlock) {
    this.blockSizeInValues = blockSizeInValues;
    this.miniBlockNumInABlock = miniBlockNumInABlock;
    double miniSize = (double) blockSizeInValues / miniBlockNumInABlock;
    Preconditions.checkArgument(miniSize % 8 == 0, "miniBlockSize must be multiple of 8, but it's " + miniSize);
    this.miniBlockSizeInValues = (int) miniSize;
  }

  public static DeltaBinaryPackingConfig readConfig(InputStream in) throws IOException {
    return new DeltaBinaryPackingConfig(BytesUtils.readUnsignedVarInt(in),
            BytesUtils.readUnsignedVarInt(in));
  }

  public BytesInput toBytesInput() {
    return BytesInput.concat(
            BytesInput.fromUnsignedVarInt(blockSizeInValues),
            BytesInput.fromUnsignedVarInt(miniBlockNumInABlock));
  }
}
