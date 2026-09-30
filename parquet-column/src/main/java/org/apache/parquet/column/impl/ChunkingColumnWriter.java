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

import org.apache.parquet.column.CdcOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.io.api.Binary;

/**
 * Ends the wrapped writer's page wherever its {@link CdcChunker} places a boundary.
 *
 * <p>A decorator rather than a change to {@link ColumnWriterBase}, so that a writer with content
 * defined chunking disabled runs exactly the code it always has.
 */
final class ChunkingColumnWriter implements ColumnWriter {

  private final ColumnWriterBase writer;
  private final CdcChunker chunker;

  ChunkingColumnWriter(ColumnWriterBase writer, ColumnDescriptor path, CdcOptions options) {
    this.writer = writer;
    this.chunker = new CdcChunker(options, path);
  }

  @Override
  public void write(int value, int repetitionLevel, int definitionLevel) {
    if (chunker.offer(value, repetitionLevel, definitionLevel)) {
      endPage();
    }
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(long value, int repetitionLevel, int definitionLevel) {
    if (chunker.offer(value, repetitionLevel, definitionLevel)) {
      endPage();
    }
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(boolean value, int repetitionLevel, int definitionLevel) {
    if (chunker.offer(value, repetitionLevel, definitionLevel)) {
      endPage();
    }
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(Binary value, int repetitionLevel, int definitionLevel) {
    if (chunker.offer(value, repetitionLevel, definitionLevel)) {
      endPage();
    }
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(float value, int repetitionLevel, int definitionLevel) {
    if (chunker.offer(value, repetitionLevel, definitionLevel)) {
      endPage();
    }
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void write(double value, int repetitionLevel, int definitionLevel) {
    if (chunker.offer(value, repetitionLevel, definitionLevel)) {
      endPage();
    }
    writer.write(value, repetitionLevel, definitionLevel);
  }

  @Override
  public void writeNull(int repetitionLevel, int definitionLevel) {
    if (chunker.offerNull(repetitionLevel, definitionLevel)) {
      endPage();
    }
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

  /** A boundary on the first value of a page has no page to end. */
  private void endPage() {
    if (writer.getValueCount() > 0) {
      writer.writePage();
    }
  }
}
