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
import org.apache.hadoop.fs.Path;

import parquet.hadoop.ParquetReader;
import parquet.tools.Main;
import parquet.tools.read.SimpleReadSupport;
import parquet.tools.read.SimpleRecord;

public class CatCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file to print to stdout"
  };

  public CatCommand() {
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

    ParquetReader<SimpleRecord> reader = null;
    try {
      PrintWriter writer = new PrintWriter(Main.out, true);
      reader = new ParquetReader<SimpleRecord>(new Path(input), new SimpleReadSupport());
      for (SimpleRecord value = reader.read(); value != null; value = reader.read()) {
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
