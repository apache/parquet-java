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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriteStore;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriteStore;
import org.apache.parquet.column.values.bloomfilter.BloomFilterWriter;
import org.apache.parquet.schema.MessageType;

public class ColumnWriteStoreV1 extends ColumnWriteStoreBase {

  public ColumnWriteStoreV1(MessageType schema, PageWriteStore pageWriteStore, ParquetProperties props) {
    super(schema, pageWriteStore, props);
  }

  @Deprecated
  public ColumnWriteStoreV1(final PageWriteStore pageWriteStore, final ParquetProperties props) {
    super(pageWriteStore, props);
  }

  public ColumnWriteStoreV1(
      MessageType schema,
      PageWriteStore pageWriteStore,
      BloomFilterWriteStore bloomFilterWriteStore,
      ParquetProperties props) {
    super(schema, pageWriteStore, bloomFilterWriteStore, props);
  }

  @Override
  ColumnWriterBase createColumnWriter(
      ColumnDescriptor path,
      PageWriter pageWriter,
      BloomFilterWriter bloomFilterWriter,
      ParquetProperties props) {
    return new ColumnWriterV1(path, pageWriter, bloomFilterWriter, props);
  }
}
