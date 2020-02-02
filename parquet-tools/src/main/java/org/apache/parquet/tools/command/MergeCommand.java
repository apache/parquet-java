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

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.tools.Main;

public class MergeCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[]{
    "<input> [<input> ...] <output>",
    "where <input> is the source parquet files/directory to be merged",
    "   <output> is the destination parquet file"
  };

  /**
   * Biggest number of input files we can merge.
   */
  private static final int MAX_FILE_NUM = 100;
  private static final long TOO_SMALL_FILE_THRESHOLD = 64 * 1024 * 1024;

  private Configuration conf;
  public static final Options OPTIONS;

  static {
    OPTIONS = new Options();
    Option mergeBlocks = Option.builder("b")
      .longOpt("block")
      .desc("Merge adjacent blocks(row groups) into one up to upper bound size limit default to 128 MB")
      .build();

    Option limit = Option.builder("l")
      .longOpt("limit")
      .desc("Upper bound for merged block(row group) size in megabytes. Default: 128 MB. Option only applicable with -b")
      .hasArg()
      .build();

    Option codec = Option.builder("c")
      .longOpt("codec")
      .desc("Compression codec name. Default: SNAPPY. Valid values: UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD. " +
        "Option only applicable with -b")
      .hasArg()
      .build();

    OPTIONS.addOption(mergeBlocks);
    OPTIONS.addOption(limit);
    OPTIONS.addOption(codec);
  }

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
      "Without -b option the command doesn't merge row groups, just places one after the other. " +
      "When used to merge many small files, the resulting file will still contain small row groups, " +
      "which usually leads to bad query performance. " +
      "To have adjacent blocks(row groups) merged together use -b option. " +
      "Blocks will be grouped into larger one until the upper bound is reached. " +
      "Default block upper bound 128 MB and default compression SNAPPY can be customized using -l and -c options";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    boolean mergeBlocks = options.hasOption('b');

    // Prepare arguments
    List<String> args = options.getArgList();
    List<Path> files = getInputFiles(args.subList(0, args.size() - 1));
    Path outputFile = new Path(args.get(args.size() - 1));
    // Merge schema and extraMeta
    ParquetMetadata parquetMetadata = mergedMetadata(files);
    ParquetFileWriter writer = new ParquetFileWriter(conf,
      parquetMetadata.getFileMetaData().getSchema(), outputFile, ParquetFileWriter.Mode.CREATE);
    PrintWriter stdOut = new PrintWriter(Main.out, true);

    if (mergeBlocks) {
      long maxRowGroupSize = options.hasOption('l')? Long.parseLong(options.getOptionValue('l')) * 1024 * 1024 : DEFAULT_BLOCK_SIZE;
      CompressionCodecName compression = options.hasOption('c') ?
        CompressionCodecName.valueOf(options.getOptionValue('c')) : CompressionCodecName.SNAPPY;

      stdOut.println("Merging files and row-groups using " + compression.name() + " for compression and " + maxRowGroupSize
        + " bytes as the upper bound for new row groups ..... ");
      mergeRowGroups(files, parquetMetadata, writer, maxRowGroupSize, compression);
    } else {
      appendRowGroups(files, parquetMetadata.getFileMetaData(), writer, stdOut);
    }
  }

  private void mergeRowGroups(List<Path> files, ParquetMetadata parquetMetadata, ParquetFileWriter writer,
                              long maxRowGroupSize, CompressionCodecName compression) throws IOException {

    boolean v2EncodingHint = parquetMetadata.getBlocks().stream()
      .flatMap(b -> b.getColumns().stream())
      .anyMatch(chunk -> {
        EncodingStats stats = chunk.getEncodingStats();
        return stats != null && stats.usesV2Pages();
      });

    List<InputFile> inputFiles = files.stream().map(f -> {
      try {
        return HadoopInputFile.fromPath(f, conf);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).collect(Collectors.toList());

    writer.start();
    writer.mergeRowGroups(inputFiles, maxRowGroupSize, v2EncodingHint, compression);
    writer.end(parquetMetadata.getFileMetaData().getKeyValueMetaData());
  }

  private void appendRowGroups(List<Path> inputFiles, FileMetaData mergedMeta, ParquetFileWriter writer, PrintWriter out) throws IOException {
    writer.start();
    boolean tooSmallFilesMerged = false;
    for (Path input : inputFiles) {
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
        "which usually leads to bad query performance! " +
        "\nConsider using merge with -b...  to merge both files and row groups");
    }
    writer.end(mergedMeta.getKeyValueMetaData());
  }

  private ParquetMetadata mergedMetadata(List<Path> inputFiles) throws IOException {
    return ParquetFileWriter.mergeMetadataFiles(inputFiles, conf);
  }

  /**
   * Get all input files.
   *
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

    for (Path inputFile : inputFiles) {
      FileSystem fs = inputFile.getFileSystem(conf);
      FileStatus status = fs.getFileStatus(inputFile);

      if (status.isDir()) {
        throw new IllegalArgumentException("Illegal parquet file: " + inputFile.toUri());
      }
    }
  }

  /**
   * Get all parquet files under partition directory.
   *
   * @param partitionDir partition directory.
   * @return parquet files to be merged.
   */
  private List<Path> getInputFilesFromDirectory(FileStatus partitionDir) throws IOException {
    FileSystem fs = partitionDir.getPath().getFileSystem(conf);
    FileStatus[] inputFiles = fs.listStatus(partitionDir.getPath(), HiddenFileFilter.INSTANCE);

    List<Path> input = new ArrayList<Path>();
    for (FileStatus f : inputFiles) {
      input.add(f.getPath());
    }
    return input;
  }

  private List<Path> parseInputFiles(List<String> input) {
    List<Path> inputFiles = new ArrayList<Path>();

    for (String name : input) {
      inputFiles.add(new Path(name));
    }

    return inputFiles;
  }
}
