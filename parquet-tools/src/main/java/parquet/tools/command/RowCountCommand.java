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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.tools.Main;
import parquet.tools.util.PrettyPrintWriter;
import parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.ArrayList;

public class RowCountCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file containing the row counts to show"
  };

  public static final Options OPTIONS;
  static {
    OPTIONS       = new Options();
    Option detail = OptionBuilder.withLongOpt("detailed")
                               .withDescription("Show the individual row counts.")
                               .create('d');
    OPTIONS.addOption(detail);
  }

  public RowCountCommand() {
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
    String input  = args[0];

    Configuration conf         = new Configuration();
    Path inputPath             = new Path(input);
    FileStatus inputFileStatus = inputPath.getFileSystem(conf).getFileStatus(inputPath);
    List<Footer> footers       = ParquetFileReader.readFooters(conf, inputFileStatus, false);

    List< Long > rowCounts = new ArrayList< Long >();
    for(Footer footer: footers){
      processFooter(footer, rowCounts);
    }
    outputCounts(rowCounts, options.hasOption('d'));
  }

  private void processFooter(Footer footer, List<Long> rowCounts) throws Exception {
    List< BlockMetaData > footerBlocks = footer.getParquetMetadata().getBlocks();
    for (BlockMetaData bmeta: footerBlocks) {
      processBlock(bmeta, rowCounts);
    }
  }

  private void processBlock(BlockMetaData blockMeta, List<Long> rowCounts) throws Exception {
    long rows = blockMeta.getRowCount();
    rowCounts.add( new Long(rows) );
  }

  private void outputCounts( List< Long > rowCounts, boolean detailed ){
    Long min  = Long.MAX_VALUE;
    Long max  = Long.MIN_VALUE;
    Long sum  = 0L;
    int count = 0;

    for(Long rc: rowCounts){
      min = ( rc < min ) ? rc : min;
      max = ( rc > max ) ? rc : max;
      sum += rc;
      count++;
    }
    double avg = (count == 0 ) ? 0 : (sum / (double) count);

    PrettyPrintWriter out = PrettyPrintWriter.stdoutPrettyPrinter()
                                             .withAutoFlush()
                                             .build();
    out.printf("Row Group Stats: [ Total:%d, Min:%d, Avg:%.2f, Max:%d, NumRowGroups:%d ]\n", sum, min, avg, max, count);
    if (detailed) {
      out.printf("Row Counts: [ %s ]\n", Joiner.on(", ").join(rowCounts) );
    }
  }
}
