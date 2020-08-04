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
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ColumnMasker;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.util.List;
import java.util.Set;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import org.apache.parquet.hadoop.util.ColumnMasker.MaskMode;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileWriter;

public class ColumnMaskingCommand extends ArgsOnlyCommand {

  private static final int MAX_COL_NUM = 100;

  public static final String[] USAGE = new String[] {
    "<mask_mode> <input> <output> [<column> ...]",

    "where <mask_mode> is mask mode: nullify, hash, redact" +
    "    <input> is the source parquet file",
    "    <output> is the destination parquet file," +
    "    [<column> ...] are the columns in the case sensitive dot format"
  };

  private Configuration conf;
  private ColumnMasker masker;

  public ColumnMaskingCommand() {
    super(4, MAX_COL_NUM + 3);
    this.conf = new Configuration();
    masker = new ColumnMasker();
  }

  public ColumnMaskingCommand(Configuration conf) {
    super(4, MAX_COL_NUM + 3);
    this.conf = conf;
    masker = new ColumnMasker();
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Replace columns in a given Parquet file with masked values and write to a new Parquet file.";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);
    List<String> args = options.getArgList();
    MaskMode mode = MaskMode.fromString(args.get(0));
    Path inPath = new Path(args.get(1));
    Path outPath = new Path(args.get(2));
    List<String> cols = args.subList(3, args.size());

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    TransParquetFileWriter writer = new TransParquetFileWriter(conf, schema, outPath, ParquetFileWriter.Mode.CREATE);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, conf), HadoopReadOptions.builder(conf).build())) {
      masker.processBlocks(reader, writer, metaData, schema, cols, mode);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }
}
