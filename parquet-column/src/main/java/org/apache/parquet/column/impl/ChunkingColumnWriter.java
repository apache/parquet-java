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
 * Ends the wrapped writer's page at every {@link ContentDefinedChunker} boundary, and applies the page
 * limits at record starts, counted from the page start rather than on the store's schedule, so that
 * they cut a chunk in the same places after an edit. A decorator, so that writers without content
 * defined chunking are unchanged.
 */
final class ChunkingColumnWriter implements ColumnWriter {

  private final ColumnWriterBase writer;
  private final ContentDefinedChunker chunker;
  private final int pageSizeThreshold;
  private final int pageValueCountThreshold;
  private final int pageRowCountLimit;

  ChunkingColumnWriter(ColumnWriterBase writer, ContentDefinedChunker chunker, ParquetProperties props) {
    this.writer = writer;
    this.chunker = chunker;
    this.pageSizeThreshold = props.getPageSizeThreshold();
    this.pageValueCountThreshold = props.getPageValueCountThreshold();
    this.pageRowCountLimit = props.getPageRowCountLimit();
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    chunker.roll(value, repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    chunker.roll(value, repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    chunker.roll(value, repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    chunker.roll(value, repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    chunker.roll(value, repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    chunker.roll(value, repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    chunker.rollNull(repetitionLevel, definitionLevel);
    beforeTriplet(repetitionLevel);
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
   * Ends the page before this triplet at a chunk boundary or where a page limit is reached, at a
   * record start only: asking the chunker consumes its match, so a match inside a record carries
   * over to the next record start. A boundary on the first value of a page has no page to end.
   */
  private void beforeTriplet(int repetitionLevel) {
    if (repetitionLevel != 0) {
      return;
    }
    boolean chunkBoundary = chunker.needNewChunk();
    if (writer.getValueCount() > 0
        && (chunkBoundary
            || writer.getPageRowCount() >= pageRowCountLimit
            || writer.getValueCount() >= pageValueCountThreshold
            || writer.getCurrentPageBufferedSize() >= pageSizeThreshold)) {
      writer.writePage();
    }
  }
}
