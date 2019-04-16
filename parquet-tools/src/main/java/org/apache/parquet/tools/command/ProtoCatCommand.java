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

import com.google.protobuf.Message;
import java.io.File;
import java.util.Objects;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.tools.Main;

public class ProtoCatCommand extends ArgsOnlyCommand {
  private static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file or directory to print to stdout"
  };

  private static final Options OPTIONS = new Options();

  public ProtoCatCommand() {
    super(1, 1);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Prints the contents of Parquet files as a stream of Protobuf messages.";
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

    processInput(new File(input));
  }

  private static void processInput(File input) throws Exception {
    if (input.isFile()) {
      processFile(input);
    } else if (input.isDirectory()) {
      for (File child : Objects.requireNonNull(input.listFiles())) {
        processInput(child);
      }
    }
  }

  private static void processFile(File inputFile) throws Exception {
    Path filePath = new Path(inputFile.getAbsolutePath());
    try (ParquetReader<Message> reader = ParquetReader.builder(new ProtoReadSupport<>(), filePath).build()) {
      for (Message msg = reader.read(); msg != null; msg = reader.read()) {
        msg.writeDelimitedTo(Main.out);
      }
    }
  }
}
