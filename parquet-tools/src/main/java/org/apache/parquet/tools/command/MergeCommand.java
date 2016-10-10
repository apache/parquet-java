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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MergeCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
          "<input> [<input> ...] <output>",
          "where <input> is the source parquet files/directory to be merged",
          "   <output> is the destination parquet file"
  };

  /**
   * Biggest number of input files we can merge.
   */
  private static final int MAX_FILE_NUM = 100;

  private Configuration conf;

  public MergeCommand() {
    super(2, MAX_FILE_NUM + 1);

    conf = new Configuration();
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    // Prepare arguments
    List<String> args = options.getArgList();
    List<Path> inputFiles = getInputFiles(args.subList(0, args.size() - 1));
    Path outputFile = new Path(args.get(args.size() - 1));

    // Merge schema and extraMeta
    FileMetaData mergedMeta = mergedMetadata(inputFiles);

    // Merge data
    ParquetFileWriter writer = new ParquetFileWriter(conf,
            mergedMeta.getSchema(), outputFile, ParquetFileWriter.Mode.CREATE);
    writer.start();
    for (Path input: inputFiles) {
      writer.appendFile(conf, input);
    }
    writer.end(mergedMeta.getKeyValueMetaData());
  }

  private FileMetaData mergedMetadata(List<Path> inputFiles) throws IOException {
    return ParquetFileWriter.mergeMetadataFiles(inputFiles, conf).getFileMetaData();
  }

  /**
   * Get all input files.
   * @param input input files or directory.
   * @return ordered input files.
   */
  private List<Path> getInputFiles(List<String> input) throws IOException {
    List<Path> inputFiles = null;

    if (input.size() == 1) {
      Path p = new Path(input.get(0));
      FileSystem fs = p.getFileSystem(conf);
      FileStatus status = fs.getFileStatus(p);

      if (status.isDir()) {
        inputFiles = getInputFilesFromDirectory(status);
      }
    } else {
      inputFiles = parseInputFiles(input);
    }

    checkParquetFiles(inputFiles);

    return inputFiles;
  }

  /**
   * Check input files basically.
   * ParquetFileReader will throw exception when reading an illegal parquet file.
   *
   * @param inputFiles files to be merged.
   * @throws IOException
   */
  private void checkParquetFiles(List<Path> inputFiles) throws IOException {
    if (inputFiles == null || inputFiles.size() <= 1) {
      throw new IllegalArgumentException("Not enough files to merge");
    }

    for (Path inputFile: inputFiles) {
      FileSystem fs = inputFile.getFileSystem(conf);
      FileStatus status = fs.getFileStatus(inputFile);

      if (status.isDir()) {
        throw new IllegalArgumentException("Illegal parquet file: " + inputFile.toUri());
      }
    }
  }

  /**
   * Get all parquet files under partition directory.
   * @param partitionDir partition directory.
   * @return parquet files to be merged.
   */
  private List<Path> getInputFilesFromDirectory(FileStatus partitionDir) throws IOException {
    FileSystem fs = partitionDir.getPath().getFileSystem(conf);
    FileStatus[] inputFiles = fs.listStatus(partitionDir.getPath(), HiddenFileFilter.INSTANCE);

    List<Path> input = new ArrayList<Path>();
    for (FileStatus f: inputFiles) {
      input.add(f.getPath());
    }
    return input;
  }

  private List<Path> parseInputFiles(List<String> input) {
    List<Path> inputFiles = new ArrayList<Path>();

    for (String name: input) {
      inputFiles.add(new Path(name));
    }

    return inputFiles;
  }
}
