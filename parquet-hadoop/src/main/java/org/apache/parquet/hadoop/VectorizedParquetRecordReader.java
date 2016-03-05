/**
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
package org.apache.parquet.hadoop;

import org.apache.parquet.io.vector.RowBatch;
import org.apache.parquet.io.vector.VectorizedReader;

import java.io.IOException;

public class VectorizedParquetRecordReader<T> implements VectorizedReader {

  private ParquetRecordReader<T> parquetRecordReader;

  public VectorizedParquetRecordReader(ParquetRecordReader<T> parquetRecordReader) {
    this.parquetRecordReader = parquetRecordReader;
  }

  @Override
  public void close() throws IOException {
    parquetRecordReader.close();
  }

  @Override
  public RowBatch nextBatch(RowBatch previous, Class clazz) throws IOException {
    throw new UnsupportedOperationException("Reading a batch of rows of complex types is not supported");
  }

  /**
   * Reads the next batch of rows. This method is used for reading primitive types
   * and does not call the converters at all.
   * @param previous a row batch object to be reused by the reader if possible
   * @return the row batch that was read
   * @throws java.io.IOException
   */
  @Override
  public RowBatch nextBatch(RowBatch previous) throws IOException {
    return parquetRecordReader.nextBatch(previous);
  }
}
