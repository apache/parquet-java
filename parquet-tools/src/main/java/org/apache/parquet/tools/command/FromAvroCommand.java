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

import java.io.File;
import java.util.Arrays;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroParquetWriter.Builder;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.tools.Main;

public class FromAvroCommand extends ArgsOnlyCommand {
  public static final String[] USAGE = new String[] {
    "<input> <output>",
    "where <input> is the avro file to write to parquet file <output>"
  };

  public static final Options OPTIONS;
  static {
      OPTIONS = new Options();
      Option ps = OptionBuilder.withLongOpt("page-size")
                               .withDescription("Set the parquet page size")
                               .withType(Integer.class)
                               .hasArg()
                               .create('p');
      Option bs = OptionBuilder.withLongOpt("block-size")
                               .withDescription("Set the parquet block size")
                               .withType(Integer.class)
                               .hasArg()
                               .create('b');
      Option cc = OptionBuilder.withLongOpt("compression-codec")
                               .withDescription("Set compression codec to use for the parquet file, one of " + Arrays.toString(CompressionCodecName.values()))
                               .hasArg()
                               .create('c');
      Option wv = OptionBuilder.withLongOpt("writer-version")
                                .withDescription("Which parquet writer version to use, one of " + Arrays.toString(WriterVersion.values()))
                               .hasArg()
                               .create('v');
      Option ud = OptionBuilder.withLongOpt("use-dictionary-encoding")
                               .withDescription("Pass this flag to enable dictionary encoding (otherwise dictionary encoding will be disabled)")
                               .create('d');

      OPTIONS.addOption(ps);
      OPTIONS.addOption(bs);
      OPTIONS.addOption(cc);
      OPTIONS.addOption(wv);
      OPTIONS.addOption(ud);
  }

  public FromAvroCommand() {
    super(2, 2);
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
    String output = args[1];

    FileReader<GenericRecord> reader = DataFileReader.openReader(
      new File(input),
      new GenericDatumReader<GenericRecord>());

    Builder<GenericRecord> builder = AvroParquetWriter.builder(new Path(output));
    builder.withSchema(reader.getSchema())
           .withDictionaryEncoding(options.hasOption('d'));
    if (options.hasOption('p')) {
      builder.withPageSize(Integer.parseInt(options.getOptionValue('p')));
    }
    if (options.hasOption('b')) {
      builder.withBlockSize(Integer.parseInt(options.getOptionValue('b')));
    }
    if (options.hasOption('c')) {
      builder.withCompressionCodec(CompressionCodecName.valueOf(options.getOptionValue('c')));
    }
    if (options.hasOption('v')) {
      builder.withWriterVersion(WriterVersion.valueOf(options.getOptionValue('v')));
    }
    ParquetWriter<GenericRecord> writer = builder.build();

    try {
      for (GenericRecord record : reader) {
        writer.write(record);
      }
    } finally {
      writer.close();
    }
  }
}
