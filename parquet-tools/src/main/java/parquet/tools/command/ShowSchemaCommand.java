/**
 * Copyright 2013 ARRIS, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.tools.command;

import java.text.DecimalFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.tools.Main;
import parquet.tools.util.MetadataUtils;
import parquet.tools.util.PrettyPrintWriter;

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
    OPTIONS.addOption(help);
  }

  public ShowSchemaCommand() {
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

    Configuration conf = new Configuration();
    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, new Path(input));
    MessageType schema = metaData.getFileMetaData().getSchema();

    Main.out.println(schema);
    if (options.hasOption('d')) {
      PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter().build();
      MetadataUtils.showDetails(out, metaData);
    }
  }
}
