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
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.tools.util.PrettyPrintWriter;
import org.apache.parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

import java.util.List;

public class ShowMetaCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option originalType = OptionBuilder.withLongOpt("originalType")
      .withDescription("Print logical types in OriginalType representation.")
      .create('o');
    OPTIONS.addOption(originalType);
  }

  public ShowMetaCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Prints the metadata of Parquet file(s)";
  }

  @Override
  public Options getOptions() {
    return OPTIONS;
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    String[] args = options.getArgs();
    String input = args[0];
    boolean showOriginalTypes = options.hasOption('o');

    Configuration conf = new Configuration();
    conf.setBoolean("parquet.strings.signed-min-max.enabled", true);
    Path inputPath = new Path(input);
    FileStatus inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath);
    List<Footer> footers = ParquetFileReader.readFooters(conf, inputFileStatus, false);

    PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter()
                                             .withAutoColumn()
                                             .withWhitespaceHandler(WhiteSpaceHandler.COLLAPSE_WHITESPACE)
                                             .withColumnPadding(1)
                                             .build();

    for(Footer f: footers) {
      out.format("file: %s%n" , f.getFile());
      MetadataUtils.showDetails(out, f.getParquetMetadata(), showOriginalTypes);
      out.flushColumns();
    }
  }
}
