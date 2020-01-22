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

import static org.apache.parquet.bytes.BytesInput.concat;

import java.io.IOException;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;

/**
 * Writes (repetition level, definition level, value) triplets and deals with
 * writing pages to the underlying layer.
 */
final class ColumnWriterV1 extends ColumnWriterBase {

  ColumnWriterV1(ColumnDescriptor path, PageWriter pageWriter, ParquetProperties props) {
    super(path, pageWriter, props);
  }

  @Override
  ValuesWriter createRLWriter(ParquetProperties props, ColumnDescriptor path) {
    return props.newRepetitionLevelWriter(path);
  }

  @Override
  ValuesWriter createDLWriter(ParquetProperties props, ColumnDescriptor path) {
    return props.newDefinitionLevelWriter(path);
  }

  @Override
  void writePage(int rowCount, int valueCount, Statistics<?> statistics, ValuesWriter repetitionLevels,
      ValuesWriter definitionLevels, ValuesWriter values) throws IOException {
    pageWriter.writePage(concat(repetitionLevels.getBytes(), definitionLevels.getBytes(), values.getBytes()),
        valueCount, rowCount, statistics, repetitionLevels.getEncoding(), definitionLevels.getEncoding(),
        values.getEncoding());
  }
}
