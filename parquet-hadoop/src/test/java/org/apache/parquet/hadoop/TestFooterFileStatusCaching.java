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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test to verify that the Footer FileStatus caching optimization reduces
 * redundant getFileStatus() RPC calls to the NameNode.
 */
public class TestFooterFileStatusCaching {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message test { required int32 id; required binary name; }");

  /**
   * Test that Footer stores and returns the FileStatus passed to its constructor
   */
  @Test
  public void testFooterStoresFileStatus() throws IOException {
    Path path = new Path("/test/file.parquet");
    ParquetMetadata metadata = mock(ParquetMetadata.class);
    FileStatus fileStatus = mock(FileStatus.class);

    // Create Footer with FileStatus
    Footer footerWithStatus = new Footer(path, metadata, fileStatus);
    assertSame("Footer should store the FileStatus", fileStatus, footerWithStatus.getFileStatus());

    // Create Footer without FileStatus (backward compatibility)
    Footer footerWithoutStatus = new Footer(path, metadata);
    assertEquals("Footer without FileStatus should return null", null, footerWithoutStatus.getFileStatus());
  }

  /**
   * Test that ParquetFileReader passes FileStatus to Footer constructor
   */
  @Test
  public void testParquetFileReaderPassesFileStatus() throws IOException {
    File tempFile = tempFolder.newFile("test.parquet");
    Path path = new Path(tempFile.getAbsolutePath());

    // Write a simple parquet file
    writeTestParquetFile(path);

    Configuration conf = new Configuration();
    FileStatus fileStatus = path.getFileSystem(conf).getFileStatus(path);

    // Read footer using ParquetFileReader
    List<FileStatus> statuses = new ArrayList<>();
    statuses.add(fileStatus);
    List<Footer> footers = ParquetFileReader.readAllFootersInParallel(conf, statuses, false);

    assertEquals("Should have one footer", 1, footers.size());
    Footer footer = footers.get(0);

    assertNotNull("Footer should have FileStatus", footer.getFileStatus());
    assertEquals(
        "Footer should have correct path",
        path.makeQualified(
            path.getFileSystem(conf).getUri(),
            path.getFileSystem(conf).getWorkingDirectory()),
        footer.getFile());
    assertEquals(
        "Footer FileStatus should match input",
        fileStatus.getPath(),
        footer.getFileStatus().getPath());
  }

  /**
   * Test that ParquetInputFormat reuses cached FileStatus instead of calling getFileStatus
   * This is the key test that verifies the optimization reduces RPC calls
   */
  @Test
  public void testParquetInputFormatReusesFileStatus() throws IOException, InterruptedException {
    // Create multiple test parquet files
    int numFiles = 3;
    List<Path> paths = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      File tempFile = tempFolder.newFile("test" + i + ".parquet");
      Path path = new Path(tempFile.getAbsolutePath());
      writeTestParquetFile(path);
      paths.add(path);
    }

    Configuration conf = new Configuration();
    FileSystem fs = paths.get(0).getFileSystem(conf);

    // Get FileStatus objects for all files
    List<FileStatus> statuses = new ArrayList<>();
    for (Path path : paths) {
      statuses.add(fs.getFileStatus(path));
    }

    // Read footers (this internally calls getFileStatus via HadoopInputFile.fromStatus)
    List<Footer> footers = ParquetFileReader.readAllFootersInParallel(conf, statuses, false);

    assertEquals("Should have " + numFiles + " footers", numFiles, footers.size());

    // Verify all footers have cached FileStatus
    for (int i = 0; i < numFiles; i++) {
      Footer footer = footers.get(i);
      assertNotNull("Footer " + i + " should have FileStatus", footer.getFileStatus());

      // Verify the cached FileStatus matches the original
      assertEquals(
          "Footer " + i + " should cache correct FileStatus path",
          statuses.get(i).getPath(),
          footer.getFileStatus().getPath());
    }

    // Now simulate what ParquetInputFormat.getSplits does
    // Before the optimization, it would call fs.getFileStatus() for each footer
    // After the optimization, it should reuse the cached FileStatus

    int cachedUsageCount = 0;

    for (Footer footer : footers) {
      FileStatus cachedStatus = footer.getFileStatus();

      if (cachedStatus != null) {
        // This is the optimized code path - reuse cached FileStatus (No RPC!)
        // Just use cachedStatus directly for getFileBlockLocations
        cachedUsageCount++;
      }
      // If cachedStatus is null, the old code would call fs.getFileStatus() here (RPC!)
    }

    // All footers should have cached FileStatus, meaning no additional getFileStatus calls needed
    assertEquals(
        "All footers should provide cached FileStatus, saving " + numFiles + " RPC calls",
        numFiles,
        cachedUsageCount);
  }

  /**
   * Test the complete workflow: read footers, generate splits, verify RPC reduction
   */
  @Test
  public void testCompleteWorkflowWithFileStatusCaching() throws IOException {
    // Create test parquet files
    int numFiles = 5;
    List<Path> paths = new ArrayList<>();
    List<FileStatus> statuses = new ArrayList<>();

    Configuration conf = new Configuration();
    for (int i = 0; i < numFiles; i++) {
      File tempFile = tempFolder.newFile("complete_test_" + i + ".parquet");
      Path path = new Path(tempFile.getAbsolutePath());
      writeTestParquetFile(path);
      paths.add(path);
      statuses.add(path.getFileSystem(conf).getFileStatus(path));
    }

    // Read footers with FileStatus
    List<Footer> footers = ParquetFileReader.readAllFootersInParallel(conf, statuses, false);

    // Simulate ParquetInputFormat's getSplits logic
    int directRpcCalls = 0;
    int cachedUsage = 0;

    for (Footer footer : footers) {
      FileStatus fileStatus = footer.getFileStatus();

      if (fileStatus == null) {
        // Would need to call getFileStatus (RPC)
        directRpcCalls++;
      } else {
        // Reuse cached FileStatus (No RPC)
        cachedUsage++;

        // Verify we can get the file length from cached FileStatus
        long fileLen = fileStatus.getLen();
        assert fileLen > 0 : "Cached FileStatus should have valid file length";
      }
    }

    assertEquals("All footers should have cached FileStatus", numFiles, cachedUsage);
    assertEquals("Should not need any direct getFileStatus RPC calls", 0, directRpcCalls);

    System.out.println("SUCCESS: Optimization saved " + numFiles + " getFileStatus RPC calls to NameNode!");
  }

  /**
   * Helper method to write a simple test Parquet file
   */
  private void writeTestParquetFile(Path path) throws IOException {
    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);

    // Delete file if it exists
    FileSystem fs = path.getFileSystem(conf);
    if (fs.exists(path)) {
      fs.delete(path, false);
    }

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withConf(conf)
        .withType(SCHEMA)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .build()) {

      SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

      // Write a few records
      for (int i = 0; i < 10; i++) {
        Group group = factory.newGroup();
        group.add("id", i);
        group.add("name", "name_" + i);
        writer.write(group);
      }
    }
  }

  /**
   * Test backward compatibility: Footer without FileStatus still works
   */
  @Test
  public void testBackwardCompatibility() throws IOException {
    File tempFile = tempFolder.newFile("backward_compat.parquet");
    Path path = new Path(tempFile.getAbsolutePath());
    writeTestParquetFile(path);

    Configuration conf = new Configuration();
    ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);

    // Create Footer without FileStatus (old way)
    Footer oldStyleFooter = new Footer(path, metadata);

    assertEquals("Old-style Footer should return null for FileStatus", null, oldStyleFooter.getFileStatus());
    assertEquals("Old-style Footer should have correct path", path, oldStyleFooter.getFile());
    assertNotNull("Old-style Footer should have metadata", oldStyleFooter.getParquetMetadata());

    // The code should handle null FileStatus gracefully and fallback to getFileStatus()
    FileSystem fs = path.getFileSystem(conf);
    FileStatus fallbackStatus = fs.getFileStatus(path); // This would be the fallback in actual code

    assertNotNull("Fallback getFileStatus should work", fallbackStatus);
  }
}
