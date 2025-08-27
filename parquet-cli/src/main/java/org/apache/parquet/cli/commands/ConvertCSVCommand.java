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

package org.apache.parquet.cli.commands;

import static org.apache.avro.generic.GenericData.Record;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.csv.AvroCSV;
import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.cli.util.Schemas;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;

@Parameters(commandDescription = "Create a file from CSV data")
public class ConvertCSVCommand extends BaseCommand {

  public ConvertCSVCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<csv path>")
  List<String> targets;

  @Parameter(
      names = {"-o", "--output"},
      description = "Output file path",
      required = true)
  String outputPath = null;

  @Parameter(
      names = {"-2", "--format-version-2", "--writer-version-2"},
      description = "Use Parquet format version 2",
      hidden = true)
  boolean v2 = false;

  @Parameter(names = "--delimiter", description = "Delimiter character")
  String delimiter = ",";

  @Parameter(names = "--escape", description = "Escape character")
  String escape = "\\";

  @Parameter(names = "--quote", description = "Quote character")
  String quote = "\"";

  @Parameter(names = "--no-header", description = "Don't use first line as CSV header")
  boolean noHeader = false;

  @Parameter(names = "--skip-lines", description = "Lines to skip before CSV start")
  int linesToSkip = 0;

  @Parameter(names = "--charset", description = "Character set name", hidden = true)
  String charsetName = Charset.defaultCharset().displayName();

  @Parameter(names = "--header", description = "Line to use as a header. Must match the CSV settings.")
  String header;

  @Parameter(names = "--require", description = "Do not allow null values for the given field")
  List<String> requiredFields;

  @Parameter(
      names = {"-s", "--schema"},
      description = "The file containing the Avro schema.")
  String avroSchemaFile;

  @Parameter(
      names = {"--compression-codec"},
      description = "A compression codec name.")
  String compressionCodecName = "GZIP";

  @Parameter(names = "--row-group-size", description = "Target row group size")
  int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

  @Parameter(names = "--page-size", description = "Target page size")
  int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Parameter(names = "--dictionary-size", description = "Max dictionary page size")
  int dictionaryPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Parameter(
      names = {"--overwrite"},
      description = "Remove any data already in the target view or dataset")
  boolean overwrite = false;

  @Parameter(
      names = {"--conf", "--property"},
      description = "Set a configuration property (format: key=value). Can be specified multiple times.")
  List<String> confProperties;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && !targets.isEmpty(), "CSV path is required.");

    if (header != null) {
      // if a header is given on the command line, don't assume one is in the file
      noHeader = true;
    }

    CSVProperties props = new CSVProperties.Builder()
        .delimiter(delimiter)
        .escape(escape)
        .quote(quote)
        .header(header)
        .hasHeader(!noHeader)
        .linesToSkip(linesToSkip)
        .charset(charsetName)
        .build();

    Schema csvSchema = null;
    if (avroSchemaFile != null) {
      csvSchema = Schemas.fromAvsc(open(avroSchemaFile));
    } else {
      Set<String> required = ImmutableSet.of();
      if (requiredFields != null) {
        required = ImmutableSet.copyOf(requiredFields);
      }

      String filename = new File(targets.get(0)).getName();
      String recordName;
      if (filename.contains(".")) {
        recordName = filename.substring(0, filename.indexOf("."));
      } else {
        recordName = filename;
      }

      // If the schema is not explicitly provided,
      // ensure that all input files share the same one.
      for (String target : targets) {
        Schema schema = AvroCSV.inferNullableSchema(recordName, open(target), props, required);
        if (csvSchema == null) {
          csvSchema = schema;
        } else if (!SchemaNormalization.toParsingForm(csvSchema)
            .equals(SchemaNormalization.toParsingForm(schema))) {
          throw new IllegalArgumentException(target + " seems to have a different schema from others. "
              + "Please specify the correct schema explicitly with the `--schema` option.");
        }
      }
    }

    // Create a configuration and apply custom properties
    Configuration conf = new Configuration(getConf());

    // Apply custom configuration properties
    if (confProperties != null) {
      for (String prop : confProperties) {
        String[] parts = prop.split("=", 2);
        if (parts.length != 2) {
          throw new IllegalArgumentException("Configuration property must be in format key=value: " + prop);
        }
        String key = parts[0].trim();
        String value = parts[1].trim();
        conf.set(key, value);
        console.debug("Set configuration property: {}={}", key, value);
      }
    }

    try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(qualifiedPath(outputPath))
        .withWriterVersion(v2 ? PARQUET_2_0 : PARQUET_1_0)
        .withWriteMode(overwrite ? ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE)
        .withCompressionCodec(Codecs.parquetCodec(compressionCodecName))
        .withDictionaryEncoding(true)
        .withDictionaryPageSize(dictionaryPageSize)
        .withPageSize(pageSize)
        .withRowGroupSize(rowGroupSize)
        .withDataModel(GenericData.get())
        .withConf(conf)
        .withSchema(csvSchema)
        .build()) {
      for (String target : targets) {
        long count = 0;
        try (AvroCSVReader<Record> reader =
            new AvroCSVReader<>(open(target), props, csvSchema, Record.class, true)) {
          for (Record record : reader) {
            writer.write(record);
            count++;
          }
        } catch (RuntimeException e) {
          throw new RuntimeException("Failed on record " + count + " in file " + target, e);
        }
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create a Parquet file from a CSV file",
        "sample.csv -o sample.parquet --schema schema.avsc",
        "# Create a Parquet file in HDFS from local CSV",
        "path/to/sample.csv -o hdfs:/user/me/sample.parquet --schema schema.avsc");
  }
}
