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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.hadoop.metadata.DefaultKeyValueMetadataMergeStrategy;
import org.apache.parquet.hadoop.metadata.KeyValueMetadataMergeStrategy;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.tools.Main;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class MergeCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
          "<input> [<input> ...] <output>",
          "where <input> is the source parquet files/directory to be merged",
          "   <output> is the destination parquet file"
  };

  private static final String DEFAULT_KEY_VALUE_MERGE_STRATEGY = new DefaultKeyValueMetadataMergeStrategy().getClass().getName();
  public static final Options OPTIONS;

  static {
    OPTIONS = new Options();
    Option mergeStrategy = Option.builder("s")
      .longOpt("mergeStrategy")
      .desc("Strategy to merge (key, value) pairs in metadata if there are multiple values for same key " +
        "(default: " + DEFAULT_KEY_VALUE_MERGE_STRATEGY + ")")
      .optionalArg(true)
      .build();

    OPTIONS.addOption(mergeStrategy);
  }

  /**
   * Biggest number of input files we can merge.
   */
  private static final int MAX_FILE_NUM = 100;
  private static final long TOO_SMALL_FILE_THRESHOLD = 64 * 1024 * 1024;

  private Configuration conf;

  public MergeCommand() {
    super(2, MAX_FILE_NUM + 1);

    conf = new Configuration();
  }

  @Override
  public Options getOptions() {
    return OPTIONS;
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Merges multiple Parquet files into one. " +
      "The command doesn't merge row groups, just places one after the other. " +
      "When used to merge many small files, the resulting file will still contain small row groups, " +
      "which usually leads to bad query performance.";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    // Prepare arguments
    List<String> args = options.getArgList();
    List<Path> inputFiles = getInputFiles(args.subList(0, args.size() - 1));
    Path outputFile = new Path(args.get(args.size() - 1));

    String mergeStrategyClass = DEFAULT_KEY_VALUE_MERGE_STRATEGY;
    if (options.hasOption('s')) {
      mergeStrategyClass = options.getOptionValue('s');
    }
    KeyValueMetadataMergeStrategy mergeStrategy = loadMergeStrategy(mergeStrategyClass);

    // Merge schema and extraMeta
    FileMetaData mergedMeta = mergedMetadata(inputFiles, mergeStrategy);
    PrintWriter out = new PrintWriter(Main.out, true);

    // Merge data
    ParquetFileWriter writer = new ParquetFileWriter(conf,
            mergedMeta.getSchema(), outputFile, ParquetFileWriter.Mode.CREATE);
    writer.start();
    boolean tooSmallFilesMerged = false;
    for (Path input: inputFiles) {
      if (input.getFileSystem(conf).getFileStatus(input).getLen() < TOO_SMALL_FILE_THRESHOLD) {
        out.format("Warning: file %s is too small, length: %d\n",
          input,
          input.getFileSystem(conf).getFileStatus(input).getLen());
        tooSmallFilesMerged = true;
      }

      writer.appendFile(HadoopInputFile.fromPath(input, conf));
    }

    if (tooSmallFilesMerged) {
      out.println("Warning: you merged too small files. " +
        "Although the size of the merged file is bigger, it STILL contains small row groups, thus you don't have the advantage of big row groups, " +
        "which usually leads to bad query performance!");
    }
    writer.end(mergedMeta.getKeyValueMetaData());
  }

  private FileMetaData mergedMetadata(List<Path> inputFiles, KeyValueMetadataMergeStrategy keyValueMetadataMergeStrategy) throws IOException {
    return ParquetFileWriter.mergeMetadataFiles(inputFiles, conf, keyValueMetadataMergeStrategy).getFileMetaData();
  }

  private KeyValueMetadataMergeStrategy loadMergeStrategy(String mergeStrategyClass) {
    Class<? extends KeyValueMetadataMergeStrategy> mergeStrategy = conf.getClass(mergeStrategyClass, null, KeyValueMetadataMergeStrategy.class);
    return ReflectionUtils.newInstance(mergeStrategy, conf);
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
