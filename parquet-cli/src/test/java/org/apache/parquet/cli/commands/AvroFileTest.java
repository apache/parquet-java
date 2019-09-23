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

import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class AvroFileTest extends ParquetFileTest {

  protected File toAvro(File inputFile) throws IOException {
    return toAvro(inputFile, "GZIP");
  }

  protected File toAvro(File inputFile, String compressionCodecName) throws IOException {
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avro");
    return toAvro(inputFile, outputFile, false, compressionCodecName);
  }

  protected File toAvro(File inputFile, File outputFile, boolean overwrite) throws IOException {
    return toAvro(inputFile, outputFile, overwrite, "GZIP");
  }

  protected File toAvro(File inputFile, File outputFile, boolean overwrite, String compressionCodecName) throws IOException {
    ToAvroCommand command = new ToAvroCommand(createLogger());
    command.targets = Arrays.asList(inputFile.getAbsolutePath());
    command.outputPath = outputFile.getAbsolutePath();
    command.compressionCodecName = compressionCodecName;
    command.overwrite = overwrite;
    command.setConf(new Configuration());
    int exitCode = command.run();
    assert(exitCode == 0);
    return outputFile;
  }
}
