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
package org.apache.parquet.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

class ConfigurationCliTest {
  @TempDir
  private Path tempDir;

  @ParameterizedTest
  @ValueSource(strings = {"--conf", "--property"})
  void propertyReachesCommand(String option) throws Exception {
    CaptureCommand command = run(option, "test.key=first=second", "capture");
    assertThat(command.getConf().get("test.key")).isEqualTo("first=second");
  }

  @Test
  void repeatedPropertiesOverrideFile() throws Exception {
    Path file = Files.writeString(tempDir.resolve("settings.properties"), "test.key=file\n");
    CaptureCommand command =
        run("--conf", "test.key=first", "--conf", "test.key=last", "--config-file", file.toString(), "capture");
    assertThat(command.getConf().get("test.key")).isEqualTo("last");
  }

  @ParameterizedTest
  @ValueSource(strings = {"-D", "--D"})
  void hadoopPropertyBeforeConf(String option) throws Exception {
    CaptureCommand command = run(option, "test.hadoop=retained", "--conf", "test.key=cli", "capture");
    assertThat(command.getConf().get("test.hadoop")).isEqualTo("retained");
    assertThat(command.getConf().get("test.key")).isEqualTo("cli");
  }

  @Test
  void attachedHadoopPropertyBeforeConf() throws Exception {
    CaptureCommand command = run("-Dtest.hadoop=retained", "--conf", "test.key=cli", "capture");
    assertThat(command.getConf().get("test.hadoop")).isEqualTo("retained");
    assertThat(command.getConf().get("test.key")).isEqualTo("cli");
  }

  @Test
  void hadoopXmlBeforeConf() throws Exception {
    Path file = Files.writeString(
        tempDir.resolve("hadoop.xml"),
        "<configuration><property><name>test.hadoop</name><value>xml</value></property></configuration>");
    CaptureCommand command = run("-conf", file.toString(), "--conf", "test.key=cli", "capture");
    assertThat(command.getConf().get("test.hadoop")).isEqualTo("xml");
    assertThat(command.getConf().get("test.key")).isEqualTo("cli");
  }

  @Test
  void hadoopValueAndCommandArgumentAreNotRewritten() throws Exception {
    CaptureCommand command = run("-D", "test.hadoop=--conf", "capture", "--value", "--conf");
    assertThat(command.getConf().get("test.hadoop")).isEqualTo("--conf");
    assertThat(command.value).isEqualTo("--conf");
  }

  @Test
  void globalOptionValueIsNotRewritten() throws Exception {
    CaptureCommand command = run("--dollar-zero", "--conf", "capture");
    assertThat(command.getConf().get("test.key")).isNull();
  }

  @Test
  void hadoopFilesystemBeforeConf() throws Exception {
    CaptureCommand command = run("-fs", "file:///", "--conf", "test.key=cli", "capture");
    assertThat(command.getConf().get("fs.defaultFS")).isEqualTo("file:///");
    assertThat(command.getConf().get("test.key")).isEqualTo("cli");
  }

  @ParameterizedTest
  @ValueSource(strings = {"-fs=file:///", "--fs=file:///"})
  void attachedHadoopOptionBeforeConf(String option) throws Exception {
    CaptureCommand command = run(option, "--conf", "test.key=cli", "capture");
    assertThat(command.getConf().get("fs.defaultFS")).isEqualTo("file:///");
    assertThat(command.getConf().get("test.key")).isEqualTo("cli");
  }

  @ParameterizedTest
  @ValueSource(strings = {"--conf", "--property"})
  void mainProcessAppliesPropertyToConversion(String option) throws Exception {
    String key = "parquet.avro.write-parquet-uuid";
    Path input = tempDir.resolve("input data.avro");
    Path output = tempDir.resolve("output data.parquet");
    Path config = Files.writeString(tempDir.resolve("writer.properties"), key + "=false\n");
    Schema schema = new Schema.Parser()
        .parse("{\"type\":\"record\",\"name\":\"record\",\"fields\":["
            + "{\"name\":\"uuid\",\"type\":{\"type\":\"string\",\"logicalType\":\"uuid\"}}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("uuid", "123e4567-e89b-12d3-a456-426614174000");
    try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      writer.create(schema, input.toFile());
      writer.append(record);
    }
    Path log = tempDir.resolve("process.log");
    Process process = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("java.class.path"),
            Main.class.getName(),
            option,
            key + "=true",
            "--config-file",
            config.toString(),
            "convert",
            "--output",
            output.toString(),
            input.toString())
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .start();
    try {
      assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
      assertThat(process.exitValue()).as(Files.readString(log)).isZero();
    } finally {
      process.destroyForcibly();
    }
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(output.toUri()), new Configuration()))) {
      assertThat(reader.getFileMetaData()
              .getSchema()
              .getType("uuid")
              .asPrimitiveType()
              .getPrimitiveTypeName())
          .isEqualTo(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
      assertThat(reader.getFileMetaData().getSchema().getType("uuid").getLogicalTypeAnnotation())
          .isEqualTo(LogicalTypeAnnotation.uuidType());
      assertThat(reader.getRowGroups()).hasSize(1);
      assertThat(reader.getRowGroups().get(0).getRowCount()).isEqualTo(1);
    }
  }

  private CaptureCommand run(String... args) throws Exception {
    Main main = new Main(LoggerFactory.getLogger(ConfigurationCliTest.class));
    CaptureCommand command = new CaptureCommand();
    main.jc.addCommand("capture", command);
    assertThat(main.runWithHadoopOptions(args)).isZero();
    return command;
  }

  @Parameters(commandDescription = "Capture the configuration delivered to a command")
  private static class CaptureCommand extends Configured implements Command {
    @Parameter(names = "--value")
    private String value;

    @Override
    public int run() {
      return 0;
    }

    @Override
    public List<String> getExamples() {
      return Collections.emptyList();
    }
  }
}
