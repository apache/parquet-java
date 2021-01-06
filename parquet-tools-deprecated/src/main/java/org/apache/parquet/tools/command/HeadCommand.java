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
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

public class HeadCommand extends ArgsOnlyCommand {
  private static final long DEFAULT = 5;

  public static final String[] USAGE = new String[]{
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public static final Options OPTIONS;

  static {
    OPTIONS = new Options();
    Option help = Option.builder("n")
      .longOpt("records")
      .desc("The number of records to show (default: " + DEFAULT + ")")
      .optionalArg(true)
      .build();

    Option json = Option.builder("j")
      .longOpt("json")
      .desc("Show records in JSON format.")
      .build();

    OPTIONS.addOption(help);
    OPTIONS.addOption(json);
  }

  public HeadCommand() {
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
    return "Prints the first n record of the Parquet file";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    long num = DEFAULT;
    if (options.hasOption('n')) {
      num = Long.parseLong(options.getOptionValue('n'));
    }

    String[] args = options.getArgs();
    String input = args[0];

    ParquetReader<SimpleRecord> reader = null;
    try {
      PrintWriter writer = new PrintWriter(Main.out, true);
      reader = ParquetReader.builder(new SimpleReadSupport(), new Path(input)).build();
      ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(input), new Configuration()));
      FileMetaData fileMetaData = fileReader.getFooter().getFileMetaData();
      JsonRecordFormatter.JsonGroupFormatter jsonFormatter = JsonRecordFormatter.fromSchema(fileMetaData.getSchema());

      for (SimpleRecord value = reader.read(); value != null && num-- > 0; value = reader.read()) {
        if (options.hasOption('j')) {
          writer.write(jsonFormatter.formatRecord(value));
        } else {
          value.prettyPrint(writer);
        }
        writer.println();
      }
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ex) {
        }
      }
    }
  }
}
