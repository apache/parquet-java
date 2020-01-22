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

import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * ColumnReader implementation for the scenario when column indexes are not used
 * (all values are read)
 */
public class ColumnReaderImpl extends ColumnReaderBase {

  /**
   * creates a reader for triplets
   * 
   * @param path the descriptor for the corresponding column
   * @param pageReader the underlying store to read from
   * @param converter a converter that materializes the values in this column in
   * the current record
   * @param writerVersion writer version string from the Parquet file being read
   */
  public ColumnReaderImpl(ColumnDescriptor path, PageReader pageReader, PrimitiveConverter converter,
      ParsedVersion writerVersion) {
    super(path, pageReader, converter, writerVersion);
    consume();
  }

  @Override
  boolean skipRL(int rl) {
    return false;
  }

  @Override
  void newPageInitialized(DataPage page) {
  }
}
