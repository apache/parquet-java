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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.apache.parquet.hadoop.util.CompressionConverter.TransParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.util.List;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class TransCompressionCommand extends ArgsOnlyCommand {

  public static final String[] USAGE = new String[] {
    "<input> <output> <codec_name>",

    "where <input> is the source parquet file",
    "    <output> is the destination parquet file," +
    "    <new_codec_name> is the codec name in the case sensitive format to be translated to, e.g. SNAPPY, GZIP, ZSTD, LZO, LZ4, BROTLI, UNCOMPRESSED"
  };

  private Configuration conf;
  private CompressionConverter compressionConverter;

  public TransCompressionCommand() {
    super(3, 3);
    this.conf = new Configuration();
    compressionConverter = new CompressionConverter();
  }

  public TransCompressionCommand(Configuration conf) {
    super(3, 3);
    this.conf = conf;
    compressionConverter = new CompressionConverter();
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Translate the compression of a given Parquet file to a new compression one to a new Parquet file.";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);
    List<String> args = options.getArgList();
    Path inPath = new Path(args.get(0));
    Path outPath = new Path(args.get(1));
    CompressionCodecName codecName = CompressionCodecName.valueOf(args.get(2));

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    ParquetFileWriter writer = new ParquetFileWriter(conf, schema, outPath, ParquetFileWriter.Mode.CREATE);
    writer.start();

    try (TransParquetFileReader reader = new TransParquetFileReader(HadoopInputFile.fromPath(inPath, conf), HadoopReadOptions.builder(conf).build())) {
      compressionConverter.processBlocks(reader, writer, metaData, schema, metaData.getFileMetaData().getCreatedBy(), codecName);
    } finally {
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }
}
