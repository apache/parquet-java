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

import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.tools.util.MetadataUtils;
import parquet.tools.util.PrettyPrintWriter;
import parquet.tools.util.PrettyPrintWriter.WhiteSpaceHandler;

import java.util.List;

public class ShowMetaCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public ShowMetaCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    String[] args = options.getArgs();
    String input = args[0];
    
    Configuration conf = new Configuration();
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
      MetadataUtils.showDetails(out, f.getParquetMetadata());
      out.flushColumns();
    }
  }
}
