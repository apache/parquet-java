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

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

public final class TransParquetFileReader extends ParquetFileReader {

  public TransParquetFileReader(InputFile file, ParquetReadOptions options) throws IOException {
    super(file, options);
  }

  public void setStreamPosition(long newPos) throws IOException {
    f.seek(newPos);
  }

  public void blockRead(byte[] data, int start, int len) throws IOException {
    f.readFully(data, start, len);
  }

  public PageHeader readPageHeader() throws IOException {
    return Util.readPageHeader(f);
  }

  public long getPos() throws IOException {
    return f.getPos();
  }

  public SeekableInputStream getStream() {
    return f;
  }
}
