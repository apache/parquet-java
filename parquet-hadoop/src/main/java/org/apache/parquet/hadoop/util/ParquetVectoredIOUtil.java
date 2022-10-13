/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.hadoop.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileRange;
import org.apache.parquet.io.ParquetFileRange;

/**
 * Utility class to perform vectored IO reads.
 */
public class ParquetVectoredIOUtil {

  /**
   * Read data in a list of file ranges.
   * @param ranges parquet file ranges.
   * @param allocate allocate function to allocate memory to hold data.
   * @param stream stream from where the data has to be read.
   * @throws IOException any IOE.
   */
  public static void readVectoredAndPopulate(final List<ParquetFileRange> ranges,
                                             final IntFunction<ByteBuffer> allocate,
                                             final FSDataInputStream stream) throws IOException {
    // Setting the parquet range as a reference.
    List<FileRange> fileRanges = ranges.stream()
      .map(range -> FileRange.createFileRange(range.getOffset(), range.getLength(), range))
      .collect(Collectors.toList());
    stream.readVectored(fileRanges, allocate);
    // As the range has been set above, there shouldn't be
    // NPE or class cast issues.
    fileRanges.forEach(fileRange -> {
      ParquetFileRange parquetFileRange = (ParquetFileRange) fileRange.getReference();
      parquetFileRange.setDataReadFuture(fileRange.getData());
    });
  }
}
