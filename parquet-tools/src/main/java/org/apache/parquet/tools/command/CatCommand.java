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
import org.apache.hadoop.fs.Path;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.tools.Main;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

public class CatCommand extends ReadCommand {
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option jsonOpt = OptionBuilder.withLongOpt("json")
                               .withDescription("Show records in JSON format.")
                               .create('j');
    OPTIONS.addOption(jsonOpt);
  }

  public CatCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
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

    Path basePath = null;
    ParquetReader<SimpleRecord> reader = null;
    try {
      basePath = new Path(input);
      PrintWriter writer = new PrintWriter(Main.out, true);
      ParquetMetadata metadata = getMetadata(basePath, filterPartitionFiles());
      reader = ParquetReader.builder(new SimpleReadSupport(), basePath).build();
      JsonRecordFormatter.JsonGroupFormatter formatter =
        JsonRecordFormatter.fromSchema(metadata.getFileMetaData().getSchema());

      for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
        if (options.hasOption('j')) {
          writer.write(formatter.formatRecord(value));
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
