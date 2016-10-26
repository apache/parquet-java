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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

public class TestReadCommand {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

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
  public void testGetMetadataNullBasePath() throws Exception {
    // test should fail for null base path
    command.getMetadata(null, command.filterPartitionFiles());
  }

  @Test(expected = IOException.class)
  public void testGetMetadataWrongBasePath() throws Exception {
    // test should fail because no Parquet metadata available for simple non-Parquet file
    File file = temp.newFile("textfile");
    Path path = new Path(file.toString());
    command.getMetadata(path, command.filterPartitionFiles());
  }

  @Test(expected = IOException.class)
  public void testGetMetadataEmptyFileStatus() throws Exception {
    // test should fail because no file statuses available for the path
    File file = temp.newFile("textfile");
    assertTrue(file.delete());
    Path path = new Path(file.toString());
    command.getMetadata(path, command.filterPartitionFiles());
  }

  @Test
  public void testGetMetadata() throws Exception {
    // read Parquet table and return first footer found
    File folder = temp.newFolder();
    Path path = new Path(folder.toString()).suffix(Path.SEPARATOR + "table.parquet");
    ParquetToolsWrite.writeParquetTable(path, 4, true);
    ParquetMetadata metadata = command.getMetadata(path, command.filterPartitionFiles());
    assertNotNull(metadata);
  }

  @Test(expected = IOException.class)
  public void testExtractMetadataWrongFooters() throws Exception {
    command.extractMetadata(null);
  }

  @Test(expected = IOException.class)
  public void testExtractMetadataEmptyFooters() throws Exception {
    command.extractMetadata(new ArrayList<Footer>());
  }

  @Test
  public void testExtractMetadata() throws Exception {
    // test verifies that we just fetch first footer metadata from provided array of footers
    // paths are dummy, files existence does not affect test
    File file1 = temp.newFile("file1");
    File file2 = temp.newFile("file2");
    List<Footer> footers = new ArrayList<Footer>();
    // we are not interested in actual values of ParquetMetadata in this test
    ParquetMetadata metadata1 = new ParquetMetadata(null, null);
    ParquetMetadata metadata2 = new ParquetMetadata(null, null);
    footers.add(new Footer(new Path(file1.toString()), metadata1));
    footers.add(new Footer(new Path(file2.toString()), metadata2));

    // we expect first metadata to be returned regardless of content
    ParquetMetadata result = command.extractMetadata(footers);
    assertSame(metadata1, result);
    assertNotSame(metadata2, result);

    // change footers to have metadata2 first
    footers.add(0, new Footer(new Path(file2.toString()), metadata2));
    result = command.extractMetadata(footers);
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
    assertEquals(true, filter.accept(new Path("/tmp/folder/_common_metadata")));
    assertEquals(true, filter.accept(new Path("/tmp/folder/_success")));
  }
}
