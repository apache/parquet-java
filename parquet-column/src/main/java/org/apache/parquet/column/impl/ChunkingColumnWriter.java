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
package org.apache.parquet.column.impl;

import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.io.api.Binary;

/**
 * Ends the wrapped writer's page wherever its {@link CdcChunker} places a boundary, and applies the
 * page limits the way Arrow C++ does under content defined chunking: measured from the page start
 * rather than on the store's schedule, so that they cut a chunk in the same places after an edit.
 *
 * <p>A decorator rather than a change to {@link ColumnWriterBase}, so that a writer with content
 * defined chunking disabled runs exactly the code it always has.
 */
final class ChunkingColumnWriter implements ColumnWriter {

  // Arrow's default write_batch_size: the size limits are checked once per batch of this many
  // levels, counted from the page start, as Arrow C++ and arrow-rs check them.
  private static final int BATCH_SIZE = 1024;

  private final ColumnWriterBase writer;
  private final CdcChunker chunker;
  private final int pageSizeThreshold;
  private final int pageValueCountThreshold;
  private final int pageRowCountLimit;

  private int levelsInBatch;

  ChunkingColumnWriter(ColumnWriterBase writer, CdcChunker chunker, ParquetProperties props) {
    this.writer = writer;
    this.chunker = chunker;
    this.pageSizeThreshold = props.getPageSizeThreshold();
    this.pageValueCountThreshold = props.getPageValueCountThreshold();
    this.pageRowCountLimit = props.getPageRowCountLimit();
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offer(value, repetitionLevel, definitionLevel), repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offer(value, repetitionLevel, definitionLevel), repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offer(value, repetitionLevel, definitionLevel), repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offer(value, repetitionLevel, definitionLevel), repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offer(value, repetitionLevel, definitionLevel), repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offer(value, repetitionLevel, definitionLevel), repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    beforeTriplet(chunker.offerNull(repetitionLevel, definitionLevel), repetitionLevel);
    writer.writeNull(repetitionLevel, definitionLevel);
  }

  @Override
  public void close() {
    writer.close();
  }

  @Override
  public long getBufferedSizeInMemory() {
    return writer.getBufferedSizeInMemory();
  }

  /**
   * Ends the page before this triplet at a chunk boundary or where a page limit is reached. Pages
   * only end at a record start: the row count limit at any, the size limits at the first once a
   * batch is full.
   */
  private void beforeTriplet(boolean chunkBoundary, int repetitionLevel) {
    if (repetitionLevel == 0) {
      if (chunkBoundary || writer.getPageRowCount() >= pageRowCountLimit) {
        endPage();
      } else if (levelsInBatch >= BATCH_SIZE) {
        if (writer.getCurrentPageBufferedSize() >= pageSizeThreshold
            || writer.getValueCount() >= pageValueCountThreshold) {
          endPage();
        } else {
          levelsInBatch = 0;
        }
      }
    }
    levelsInBatch++;
  }

  /** A boundary on the first value of a page has no page to end. */
  private void endPage() {
    if (writer.getValueCount() > 0) {
      writer.writePage();
    }
    levelsInBatch = 0;
  }
}
