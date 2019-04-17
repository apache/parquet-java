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

import com.google.protobuf.AbstractMessageLite;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.proto.ProtoReadSupport;
import org.apache.parquet.tools.Main;

public class ProtoCatCommand extends ArgsOnlyCommand {
  private static final String[] USAGE = new String[] {
    "<input>",
    "where <input> is the parquet file or directory to print to stdout"
  };

  private static final Options OPTIONS = new Options();

  static {
    OPTIONS.addOption("c", "class", true, "Protobuf message class name.");
    OPTIONS.addOption("p", "projection", true, "Requested projection.");
    OPTIONS.addOption("b", "buffer-size", true, "Buffer size in kilobytes (256KB by default).");
  }

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

    Configuration conf = new Configuration();
    ProtoReadSupport.setProtoRecordConverterBuildBefore(conf, true);

    if (options.hasOption('c')) {
      String msgClassName = options.getOptionValue('c');
      ProtoReadSupport.setProtobufClass(conf, msgClassName);
    }

    if (options.hasOption('p')) {
      String projection = options.getOptionValue('p');
      ProtoReadSupport.setRequestedProjection(conf, projection);
    }

    int bufferSizeKB = 256;
    if (options.hasOption('b')) {
      bufferSizeKB = Integer.parseInt(options.getOptionValue('b'));
    }

    final CodedOutputStream codedOutput =
      CodedOutputStream.newInstance(Main.out, bufferSizeKB * 1024);

    processInput(new File(input), conf, codedOutput);
  }

  private static void processInput(File input, Configuration conf, CodedOutputStream codedOutput) throws Exception {
    if (!input.exists() || !input.canRead())
      return;

    if (input.isFile()) {
      processFile(input, conf, codedOutput);
    } else if (input.isDirectory()) {
      File[] files = Objects.requireNonNull(input.listFiles());
      Arrays.sort(files, Comparator.comparing(File::getName));

      for (File child : files) {
        processInput(child, conf, codedOutput);
      }
    }
  }

  private static void processFile(File inputFile, Configuration conf, CodedOutputStream codedOutput) throws Exception {
    Path filePath = new Path(inputFile.getAbsolutePath());
    try (ParquetReader<Message> reader =
           ParquetReader.builder(new ProtoReadSupport<>(), filePath).withConf(conf).build()) {
      for (Message msg = reader.read(); msg != null; msg = reader.read()) {
        writeDelimited(msg, codedOutput);
      }
      codedOutput.flush();
    }
  }

  /**
   * Based on {@link AbstractMessageLite#writeDelimitedTo(OutputStream)}.
   *
   * @param msg Message.
   * @param codedOutput Output.
   * @throws IOException If failed.
   */
  private static void writeDelimited(Message msg, CodedOutputStream codedOutput) throws IOException {
    final int serialized = msg.getSerializedSize();
    codedOutput.writeUInt32NoTag(serialized);
    msg.writeTo(codedOutput);
  }
}
