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

import java.nio.file.Path;
import org.apache.parquet.Version;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CliBehaviorTest extends CliTestBase {

  @ParameterizedTest
  @ValueSource(strings = {"help", "-h", "-help", "--help"})
  void printsGeneralHelp(String command) throws Exception {
    CliResult result = cli(command);

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains("Usage: parquet [options] [command] [command options]", "Commands:", "schema", "meta");
    assertThat(result.output()).doesNotContain("--dollar-zero");
  }

  @Test
  void printsCommandHelp() throws Exception {
    CliResult result = cli("help", "schema");

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains("Usage: parquet [general options] schema", "--output", "--overwrite");
    assertThat(result.output()).doesNotContain("Commands:");
  }

  @ParameterizedTest
  @ValueSource(strings = {"-h", "-help", "--help"})
  void printsHelpAfterCommand(String option) throws Exception {
    CliResult result = cli("rewrite", option);

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).contains("Usage: parquet [general options] rewrite", "--input", "--output");
    assertThat(result.output()).doesNotContain("Argument error");
  }

  @ParameterizedTest
  @ValueSource(strings = {"version", "-version", "--version"})
  void printsVersion(String command) throws Exception {
    CliResult result = cli(command);

    assertThat(result.exitCode()).isZero();
    assertThat(result.output()).isEqualTo(Version.FULL_VERSION);
  }

  @Test
  void printsHelpAndFailsWithoutCommand() throws Exception {
    CliResult result = cli();

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("Usage: parquet [options] [command] [command options]");
  }

  @Test
  void rejectsUnknownCommand() throws Exception {
    CliResult result = cli("not-a-command");

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("not-a-command");
  }

  @Test
  void rejectsHelpForUnknownCommand() throws Exception {
    CliResult result = cli("help", "not-a-command");

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("Unknown command: not-a-command", "Usage: parquet");
  }

  @Test
  void rejectsUnknownOption() throws Exception {
    CliResult result = cli("rewrite", "--not-an-option");

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("--not-an-option");
  }

  @Test
  void rejectsMissingOptionValue() throws Exception {
    CliResult result = cli("schema", "--output");

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("--output");
  }

  @Test
  void rejectsMissingInput() throws Exception {
    CliResult result = cli("schema");

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("Argument error: Parquet file is required.");
  }

  @Test
  void printsCommandHelpForMissingRequiredOptions() throws Exception {
    CliResult result = cli("rewrite");

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("Usage: parquet [general options] rewrite", "--input", "--output");
  }

  @Test
  void reportsMissingInputFile() throws Exception {
    Path input = getTempFolder().toPath().resolve("missing input.parquet");

    CliResult result = cli("schema", input.toString());

    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.output()).contains("Unknown error", "FileNotFoundException", "missing input.parquet");
  }
}
