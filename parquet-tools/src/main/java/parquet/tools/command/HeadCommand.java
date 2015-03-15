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
package parquet.tools.command;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetReader;
import parquet.tools.Main;
import parquet.tools.read.SimpleReadSupport;
import parquet.tools.read.SimpleRecord;

public class HeadCommand extends ArgsOnlyCommand {
  private static final long DEFAULT = 5;

  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS = new Options();
    Option help = OptionBuilder.withLongOpt("records")
                               .withDescription("The number of records to show (default: " + DEFAULT + ")")
                               .hasOptionalArg()
                               .create('n');
    OPTIONS.addOption(help);
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
      reader = new ParquetReader<SimpleRecord>(new Path(input), new SimpleReadSupport());
      for (SimpleRecord value = reader.read(); value != null && num-- > 0; value = reader.read()) {
        value.prettyPrint(writer);
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
