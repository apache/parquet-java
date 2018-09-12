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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.util.PrettyPrintWriter;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class ShowSchemaCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file containing the schema to show"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option help = OptionBuilder.withLongOpt("detailed")
                               .withDescription("Show detailed information about the schema.")
                               .create('d');
    Option originalType = OptionBuilder.withLongOpt("originalType")
      .withDescription("Print logical types in OriginalType representation.")
      .create('o');
    OPTIONS.addOption(help);
    OPTIONS.addOption(originalType);
  }

  public ShowSchemaCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Prints the schema of Parquet file(s)";
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

    Configuration conf = new Configuration();
    ParquetMetadata metaData;

    Path path = new Path(input);
    FileSystem fs = path.getFileSystem(conf);
    Path file;
    if (fs.isDirectory(path)) {
      FileStatus[] statuses = fs.listStatus(path, HiddenFileFilter.INSTANCE);
      if (statuses.length == 0) {
        throw new RuntimeException("Directory " + path.toString() + " is empty");
      }
      file = statuses[0].getPath();
    } else {
      file = path;
    }
    metaData = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();

    Main.out.println(schema);
    if (options.hasOption('d')) {
      boolean showOriginalTypes = options.hasOption('o');
      PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter().build();
      MetadataUtils.showDetails(out, metaData, showOriginalTypes);
    }
  }
}
