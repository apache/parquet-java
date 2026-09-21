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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.parquet.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestInputFormatFileStatusReuse {
  private static final String TRACKING_SCHEME = "tracking";
  private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("message test { required int32 id; }");

  @TempDir
  private java.nio.file.Path tempDir;

  @Test
  public void reusesListedFileStatusAfterReadingAndCachingFooters() throws Exception {
    File parquetFile = writeParquetFile();
    TestContext context = newTestContext(parquetFile);
    FixedStatusInputFormat inputFormat = new FixedStatusInputFormat(context.status);

    assertUsesListedStatus(inputFormat, context, context.status);
    assertThat(inputFormat.getFooterReadCount()).isEqualTo(1);

    FileStatus refreshedStatus = new FileStatus(context.status);
    inputFormat.setStatus(refreshedStatus);
    context.fileSystem.clearObservations();
    assertUsesListedStatus(inputFormat, context, refreshedStatus);
    assertThat(inputFormat.getFooterReadCount())
        .as("the second split plan should use the cached footer")
        .isEqualTo(1);
  }

  @Test
  public void reusesListedFileStatusWithSummaryFileFooters() throws Exception {
    File parquetFile = writeParquetFile();
    writeSummaryFiles(parquetFile);
    TestContext context = newTestContext(parquetFile);
    FixedStatusInputFormat inputFormat = new FixedStatusInputFormat(context.status);

    assertUsesListedStatus(inputFormat, context, context.status);

    Path metadataPath = new Path(context.status.getPath().getParent(), ParquetFileWriter.PARQUET_METADATA_FILE);
    assertThat(context.fileSystem.getOpenCount(metadataPath))
        .as("the summary metadata should supply the footer")
        .isPositive();
    assertThat(context.fileSystem.getOpenCount(context.status.getPath()))
        .as("the data file should not be opened when its footer comes from summary metadata")
        .isZero();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void looksUpFileStatusForFootersWithoutListedStatuses() throws Exception {
    File parquetFile = writeParquetFile();
    TestContext context = newTestContext(parquetFile);
    Path localPath = new Path(parquetFile.toURI());
    ParquetMetadata metadata = ParquetFileReader.readFooter(new Configuration(), localPath);
    Footer footer = new Footer(context.status.getPath(), metadata);
    FixedStatusInputFormat inputFormat = new FixedStatusInputFormat(Collections.emptyList());

    List<ParquetInputSplit> splits =
        inputFormat.getSplits(context.job.getConfiguration(), Collections.singletonList(footer));

    assertThat(splits).hasSize(1);
    assertThat(context.fileSystem.getFileStatusCount(context.status.getPath()))
        .isEqualTo(1);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void reusesProvidedFileStatusWithDeprecatedSplitPlanning() throws Exception {
    File parquetFile = writeParquetFile();
    TestContext context = newTestContext(parquetFile);
    FixedStatusInputFormat inputFormat = new FixedStatusInputFormat(context.status);
    List<Footer> footers =
        inputFormat.getFooters(context.job.getConfiguration(), Collections.singletonList(context.status));

    context.fileSystem.clearObservations();
    List<ParquetInputSplit> splits = inputFormat.getSplits(context.job.getConfiguration(), footers);

    assertThat(splits).hasSize(1);
    assertThat(context.fileSystem.getFileStatusCount(context.status.getPath()))
        .isZero();
    assertThat(context.fileSystem.getBlockLocationStatus()).isSameAs(context.status);
  }

  private void assertUsesListedStatus(
      FixedStatusInputFormat inputFormat, TestContext context, FileStatus expectedStatus) throws IOException {
    List<InputSplit> splits = inputFormat.getSplits(context.job);

    assertThat(splits).hasSize(1);
    assertThat(context.fileSystem.getFileStatusCount(context.status.getPath()))
        .as("split planning should not request status for a file returned by listStatus")
        .isZero();
    assertThat(context.fileSystem.getBlockLocationStatus())
        .as("block location lookup should use the FileStatus returned by listStatus")
        .isSameAs(expectedStatus);
  }

  private TestContext newTestContext(File parquetFile) throws Exception {
    Configuration configuration = new Configuration();
    configuration.setClass("fs." + TRACKING_SCHEME + ".impl", TrackingFileSystem.class, FileSystem.class);
    Job job = Job.getInstance(configuration);
    ParquetInputFormat.setTaskSideMetaData(job, false);

    Path path = new Path(TRACKING_SCHEME, UUID.randomUUID().toString(), parquetFile.getAbsolutePath());
    TrackingFileSystem fileSystem = (TrackingFileSystem) path.getFileSystem(configuration);
    FileStatus status = fileSystem.getFileStatus(path);
    fileSystem.clearObservations();
    return new TestContext(job, fileSystem, status);
  }

  private File writeParquetFile() throws IOException {
    File file = tempDir.resolve("part-00000.parquet").toFile();
    Configuration configuration = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, configuration);

    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.toURI()))
        .withConf(configuration)
        .withType(SCHEMA)
        .build()) {
      writer.write(new SimpleGroupFactory(SCHEMA).newGroup().append("id", 1));
    }
    return file;
  }

  private void writeSummaryFiles(File parquetFile) throws IOException {
    Configuration configuration = new Configuration();
    Path file = new Path(parquetFile.toURI());
    FileStatus status = file.getFileSystem(configuration).getFileStatus(file);
    ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, status);
    Footer footer = new Footer(file, metadata);
    ParquetFileWriter.writeMetadataFile(
        configuration,
        new Path(parquetFile.getParentFile().toURI()),
        Collections.singletonList(footer),
        JobSummaryLevel.ALL);
  }

  private static final class TestContext {
    private final Job job;
    private final TrackingFileSystem fileSystem;
    private final FileStatus status;

    private TestContext(Job job, TrackingFileSystem fileSystem, FileStatus status) {
      this.job = job;
      this.fileSystem = fileSystem;
      this.status = status;
    }
  }

  private static final class FixedStatusInputFormat extends ParquetInputFormat<Group> {
    private List<FileStatus> statuses;
    private int footerReadCount;

    private FixedStatusInputFormat(FileStatus status) {
      this(Collections.singletonList(status));
    }

    private FixedStatusInputFormat(List<FileStatus> statuses) {
      super(GroupReadSupport.class);
      this.statuses = statuses;
    }

    @Override
    protected List<FileStatus> listStatus(org.apache.hadoop.mapreduce.JobContext jobContext) {
      return statuses;
    }

    @Override
    public List<Footer> getFooters(Configuration configuration, Collection<FileStatus> statuses)
        throws IOException {
      footerReadCount++;
      return super.getFooters(configuration, statuses);
    }

    private int getFooterReadCount() {
      return footerReadCount;
    }

    private void setStatus(FileStatus status) {
      statuses = Collections.singletonList(status);
    }
  }

  public static final class TrackingFileSystem extends RawLocalFileSystem {
    private final RawLocalFileSystem delegate = new RawLocalFileSystem();
    private final List<Path> fileStatusPaths = Collections.synchronizedList(new ArrayList<Path>());
    private final List<Path> openedPaths = Collections.synchronizedList(new ArrayList<Path>());
    private URI uri = URI.create(TRACKING_SCHEME + ":///");
    private volatile FileStatus blockLocationStatus;

    @Override
    public void initialize(URI name, Configuration configuration) throws IOException {
      super.initialize(name, configuration);
      String authority = name.getAuthority() == null ? "" : name.getAuthority();
      uri = URI.create(name.getScheme() + "://" + authority + "/");
      delegate.initialize(URI.create("file:///"), configuration);
    }

    @Override
    public URI getUri() {
      return uri == null ? URI.create(TRACKING_SCHEME + ":///") : uri;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
      openedPaths.add(path);
      return delegate.open(toLocalPath(path), bufferSize);
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
      fileStatusPaths.add(path);
      FileStatus status = new FileStatus(delegate.getFileStatus(toLocalPath(path)));
      status.setPath(path);
      return status;
    }

    @Override
    public BlockLocation[] getFileBlockLocations(FileStatus status, long start, long length) throws IOException {
      blockLocationStatus = status;
      return super.getFileBlockLocations(status, start, length);
    }

    private Path toLocalPath(Path path) {
      return new Path(path.toUri().getPath());
    }

    private int getFileStatusCount(Path path) {
      synchronized (fileStatusPaths) {
        return Collections.frequency(fileStatusPaths, path);
      }
    }

    private int getOpenCount(Path path) {
      synchronized (openedPaths) {
        return Collections.frequency(openedPaths, path);
      }
    }

    private FileStatus getBlockLocationStatus() {
      return blockLocationStatus;
    }

    private void clearObservations() {
      fileStatusPaths.clear();
      openedPaths.clear();
      blockLocationStatus = null;
    }
  }
}
