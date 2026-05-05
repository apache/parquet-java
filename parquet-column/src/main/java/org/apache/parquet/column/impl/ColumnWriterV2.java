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

import java.io.IOException;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geospatial.GeospatialStatistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.DevNullValuesWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;

/**
 * Writes (repetition level, definition level, value) triplets and deals with writing pages to the underlying layer.
 */
final class ColumnWriterV2 extends ColumnWriterBase {

  // Extending the original implementation to not to write the size of the data as the original writer would
  private static class RLEWriterForV2 extends RunLengthBitPackingHybridValuesWriter {
    public RLEWriterForV2(RunLengthBitPackingHybridEncoder encoder) {
      super(encoder);
    }

    @Override
    public BytesInput getBytes() {
      try {
        return encoder.toBytes();
      } catch (IOException e) {
        throw new ParquetEncodingException(e);
      }
    }
  }

  private static final ValuesWriter NULL_WRITER = new DevNullValuesWriter();

  ColumnWriterV2(ColumnDescriptor path, PageWriter pageWriter, ParquetProperties props) {
    super(path, pageWriter, props);
  }

  ColumnWriterV2(
      ColumnDescriptor path,
      PageWriter pageWriter,
      BloomFilterWriter bloomFilterWriter,
      ParquetProperties props) {
    super(path, pageWriter, bloomFilterWriter, props);
  }

  @Override
  ValuesWriter createRLWriter(ParquetProperties props, ColumnDescriptor path) {
    return path.getMaxRepetitionLevel() == 0
        ? NULL_WRITER
        : new RLEWriterForV2(props.newRepetitionLevelEncoder(path));
  }

  @Override
  ValuesWriter createDLWriter(ParquetProperties props, ColumnDescriptor path) {
    return path.getMaxDefinitionLevel() == 0
        ? NULL_WRITER
        : new RLEWriterForV2(props.newDefinitionLevelEncoder(path));
  }

  @Override
  void writePage(
      int rowCount,
      int valueCount,
      Statistics<?> statistics,
      SizeStatistics sizeStatistics,
      GeospatialStatistics geospatialStatistics,
      ValuesWriter repetitionLevels,
      ValuesWriter definitionLevels,
      ValuesWriter values)
      throws IOException {
    // TODO: rework this API. The bytes shall be retrieved before the encoding (encoding might be different
    // otherwise)
    BytesInput bytes = values.getBytes();
    Encoding encoding = values.getEncoding();
    pageWriter.writePageV2(
        rowCount,
        Math.toIntExact(statistics.getNumNulls()),
        valueCount,
        repetitionLevels.getBytes(),
        definitionLevels.getBytes(),
        encoding,
        bytes,
        statistics,
        sizeStatistics,
        geospatialStatistics);
  }
}
