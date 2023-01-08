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
package org.apache.parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;

public class TestParquetReaderEmptyBlock {

  private static final Path EMPTY_BLOCK_FILE_1 = createPathFromCP("/test-empty-row-group_1.parquet");

  private static final Path EMPTY_BLOCK_FILE_2 = createPathFromCP("/test-empty-row-group_2.parquet");

  private static Path createPathFromCP(String path) {
    try {
      return new Path(TestParquetReaderEmptyBlock.class.getResource(path).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReadOnlyEmptyBlock() throws IOException {
    Configuration conf = new Configuration();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, EMPTY_BLOCK_FILE_1);

    // The parquet file contains only one empty row group
    Assert.assertEquals(1, readFooter.getBlocks().size());

    // The empty block is skipped
    try (ParquetFileReader r = new ParquetFileReader(conf, EMPTY_BLOCK_FILE_1, readFooter)) {
      Assert.assertNull(r.readNextRowGroup());
    }
  }

  @Test
  public void testSkipEmptyBlock() throws IOException {
    Configuration conf = new Configuration();
    ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, EMPTY_BLOCK_FILE_2);

    // The parquet file contains three row groups, the second one is empty
    Assert.assertEquals(3, readFooter.getBlocks().size());

    // Second row group is empty and skipped
    try (ParquetFileReader r = new ParquetFileReader(conf, EMPTY_BLOCK_FILE_2, readFooter)) {
      PageReadStore pages = null;
      pages = r.readNextRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(1, pages.getRowCount());

      pages = r.readNextRowGroup();
      Assert.assertNotNull(pages);
      Assert.assertEquals(1, pages.getRowCount());

      pages = r.readNextRowGroup();
      Assert.assertNull(pages);
    }
  }

}
