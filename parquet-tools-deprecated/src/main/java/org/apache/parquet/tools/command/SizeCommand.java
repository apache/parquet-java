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

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.tools.Main;

public class SizeCommand extends ArgsOnlyCommand {
  private FileStatus[] inputFileStatuses;
  private Configuration conf;
  private Path inputPath;
  private PrintWriter out;
  private static final double ONE_KB = 1024;
  private static final double ONE_MB = ONE_KB * 1024;
  private static final double ONE_GB = ONE_MB * 1024;
  private static final double ONE_TB = ONE_GB * 1024;
  private static final double ONE_PB = ONE_TB * 1024;

  public static final String[] USAGE = new String[] {
          "<input>",
          "where <input> is the parquet file to get size & human readable size to stdout"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option help = OptionBuilder.withLongOpt("pretty")
            .withDescription("Pretty size")
            .create('p');
    OPTIONS.addOption(help);
    Option uncompressed = OptionBuilder.withLongOpt("uncompressed")
            .withDescription("Uncompressed size")
            .create('u');
    OPTIONS.addOption(uncompressed);
    Option detailed = OptionBuilder.withLongOpt("detailed")
            .withDescription("Detailed size of each matching file")
            .create('d');
    OPTIONS.addOption(detailed);
  }

  public SizeCommand() {
    super(1, 1);
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
    return "Prints the size of Parquet file(s)";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    String[] args = options.getArgs();
    String input = args[0];
    out = new PrintWriter(Main.out, true);
    inputPath = new Path(input);
    conf = new Configuration();
    inputFileStatuses = inputPath.getFileSystem(conf).globStatus(inputPath);
    long size = 0;
    for (FileStatus fs : inputFileStatuses) {
      long fileSize = 0;
      for (Footer f : ParquetFileReader.readFooters(conf, fs, false)) {
        for (BlockMetaData b : f.getParquetMetadata().getBlocks()) {
          size += (options.hasOption('u') ? b.getTotalByteSize() : b.getCompressedSize());
          fileSize += (options.hasOption('u') ? b.getTotalByteSize() : b.getCompressedSize());
        }
      }
      if (options.hasOption('d')) {
        if (options.hasOption('p')) {
          out.format("%s: %s\n", fs.getPath().getName(), getPrettySize(fileSize));
        }
        else {
          out.format("%s: %d bytes\n", fs.getPath().getName(), fileSize);
        }
      }
    }

    if (options.hasOption('p')) {
      out.format("Total Size: %s", getPrettySize(size));
    }
    else {
      out.format("Total Size: %d bytes", size);
    }
    out.println();
  }

  public String getPrettySize(long bytes){
    if (bytes/ONE_KB < 1) {
      return  String.format("%d", bytes) + " bytes";
    }
    if (bytes/ONE_MB < 1) {
      return String.format("%.3f", bytes/ONE_KB) + " KB";
    }
    if (bytes/ONE_GB < 1) {
      return String.format("%.3f", bytes/ONE_MB) + " MB";
    }
    if (bytes/ONE_TB < 1) {
      return String.format("%.3f", bytes/ONE_GB) + " GB";
    }
    if (bytes/ONE_PB < 1) {
      return String.format("%.3f", bytes/ONE_TB) + " TB";
    }
    return String.format("%.3f", bytes/ONE_PB) + " PB";
  }
}
