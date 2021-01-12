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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.ColumnPruner;

import java.util.List;

public class PruneColumnsCommand extends ArgsOnlyCommand {

  public static final String[] USAGE = new String[] {
    "<input> <output> [<column> ...]",

    "where <input> is the source parquet file",
    "    <output> is the destination parquet file," +
    "    [<column> ...] are the columns in the case sensitive dot format" +
      " to be pruned, for example a.b.c"
  };

  /**
   * Biggest number of columns we can prune.
   */
  private static final int MAX_COL_NUM = 100;
  private Configuration conf;

  public PruneColumnsCommand() {
    super(3, MAX_COL_NUM + 1);

    conf = new Configuration();
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Prune column(s) in a Parquet file and save it to a new file. " +
      "The columns left are not changed.";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    List<String> args = options.getArgList();
    Path inputFile = new Path(args.get(0));
    Path outputFile = new Path(args.get(1));
    List<String> cols = args.subList(2, args.size());
    ColumnPruner columnPruner = new ColumnPruner();

    columnPruner.pruneColumns(conf, inputFile, outputFile, cols);
  }
}

