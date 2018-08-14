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
package org.apache.parquet.hadoop.util;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class BlocksCombiner {

  public static List<SmallBlocksUnion> combineLargeBlocks(List<ParquetFileReader> readers, long maxBlockSize) {
    List<SmallBlocksUnion> blocks = new ArrayList<>();
    long largeBlockSize = 0;
    long largeBlockRecords = 0;
    List<SmallBlock> smallBlocks = new ArrayList<>();
    for (ParquetFileReader reader : readers) {
      for (int blockIndex = 0; blockIndex < reader.blocksCount(); blockIndex++) {
        BlockMetaData block = reader.getBlockMetaData(blockIndex);
        if (!smallBlocks.isEmpty() && largeBlockSize + block.getTotalByteSize() > maxBlockSize) {
          blocks.add(new SmallBlocksUnion(smallBlocks, largeBlockRecords));
          smallBlocks = new ArrayList<>();
          largeBlockSize = 0;
          largeBlockRecords = 0;
        }
        largeBlockSize += block.getTotalByteSize();
        largeBlockRecords += block.getRowCount();
        smallBlocks.add(new SmallBlock(reader, blockIndex));
      }
    }
    if (!smallBlocks.isEmpty()) {
      blocks.add(new SmallBlocksUnion(smallBlocks, largeBlockRecords));
    }
    return unmodifiableList(blocks);
  }

  public static void closeReaders(List<ParquetFileReader> readers) {
    readers.forEach(r -> {
      try {
        r.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static class SmallBlocksUnion {
    private final List<SmallBlock> blocks;
    private final long rowCount;

    public SmallBlocksUnion(List<SmallBlock> blocks, long rowCount) {
      this.blocks = blocks;
      this.rowCount = rowCount;
    }

    public List<SmallBlock> getBlocks() {
      return blocks;
    }

    public long getRowCount() {
      return rowCount;
    }
  }

  public static class SmallBlock {
    private final ParquetFileReader reader;
    private final int blockIndex;

    public SmallBlock(ParquetFileReader reader, int blockIndex) {
      this.reader = reader;
      this.blockIndex = blockIndex;
    }

    public ParquetFileReader getReader() {
      return reader;
    }

    public int getBlockIndex() {
      return blockIndex;
    }
  }
}
