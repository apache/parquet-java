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

package org.apache.parquet.tools.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class TestReadCommand {
  // dummy test command for metadata tests
  private ReadCommand command = null;

  @Before
  public void setUp() {
    command = new ReadCommand(1, 1) {
      @Override
      public String[] getUsageDescription() {
        return null;
      }

      @Override
      public Options getOptions() {
        return null;
      }

      @Override
      public void execute(CommandLine options) throws Exception { }
    };
  }

  @Test(expected = IOException.class)
  public void testGetMetadataWrongBasePath() throws Exception {
    // test should fail because no Parquet metadata available for text file
    Path file = new Path(getClass().getResource("/org/apache/parquet/tools/build/readme").toURI());
    command.getMetadata(file, command.filterPartitionFiles());
  }

  @Test(expected = IOException.class)
  public void testGetMetadataEmptyFileStatus() throws Exception {
    // test should fail because no file statuses available for the path
    Path file = new Path(getClass().getResource("/org/apache/parquet/tools/build/readme").toURI())
      .suffix("-empty");
    command.getMetadata(file, command.filterPartitionFiles());
  }

  @Test
  public void testGetMetadata() throws Exception {
    // read Parquet table and return first footer found
    Path file = new Path(getClass()
      .getResource("/org/apache/parquet/tools/build/sample.parquet/_SUCCESS").toURI())
      .getParent();
    ParquetMetadata metadata = command.getMetadata(file, command.filterPartitionFiles());
    assertNotNull(metadata);
  }

  @Test(expected = IOException.class)
  public void testExtractMetadataWrongBasePath() throws Exception {
    command.extractMetadata(new ArrayList<Footer>(), null);
  }

  @Test(expected = IOException.class)
  public void testExtractMetadataWrongFooters() throws Exception {
    command.extractMetadata(null, new Path("/tmp/folder"));
  }

  @Test(expected = IOException.class)
  public void testExtractMetadataEmptyFooters() throws Exception {
    command.extractMetadata(new ArrayList<Footer>(), new Path("/tmp/folder"));
  }

  @Test
  public void testExtractMetadata() throws Exception {
    List<Footer> footers = new ArrayList<Footer>();
    // we are not interested in actual values of ParquetMetadata in this test
    ParquetMetadata metadata1 = new ParquetMetadata(null, null);
    ParquetMetadata metadata2 = new ParquetMetadata(null, null);
    footers.add(new Footer(new Path("/tmp/folder/file1"), metadata1));
    footers.add(new Footer(new Path("/tmp/folder/file2"), metadata2));

    // we expect first metadata to be returned regardless of content
    ParquetMetadata result = command.extractMetadata(footers, new Path("/tmp/folder"));
    assertSame(metadata1, result);
    assertNotSame(metadata2, result);

    // change footers to have metadata2 first
    footers.add(0, new Footer(new Path("/tmp/folder/file2"), metadata2));
    result = command.extractMetadata(footers, new Path("/tmp/folder"));
    assertSame(metadata2, result);
    assertNotSame(metadata1, result);
  }

  @Test
  public void testFilterPartitionFiles() throws Exception {
    PathFilter filter = command.filterPartitionFiles();
    assertEquals(false, filter.accept(null));
    assertEquals(false, filter.accept(new Path("/tmp/folder/_SUCCESS")));
    assertEquals(true, filter.accept(new Path("/tmp/folder")));
    assertEquals(true, filter.accept(new Path("/tmp/folder/_metadata")));
    assertEquals(true, filter.accept(new Path("/tmp/folder/_success")));
  }
}
