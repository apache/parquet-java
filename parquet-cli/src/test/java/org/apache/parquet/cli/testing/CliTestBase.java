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
package org.apache.parquet.cli.testing;

import org.apache.parquet.cli.commands.ParquetFileTest;

/**
 * Base class for CLI integration tests with an API for testing command output.
 *
 * Developer Usage Examples:
 *
 * // Basic command execution and assertion
 * cli("schema file.parquet")
 *     .ok()
 *     .outputContains("int32_field", "int64_field");
 *
 * // Test help output
 * cli("help size-stats")
 *     .ok()
 *     .matchOutputFromFile("expected-help.txt");
 *
 * // Test error conditions
 * cli("invalid-command")
 *     .fails(1)
 *     .outputContains("Unknown command");
 *
 * // Test command with multiple arguments
 * cli("size-stats parquetFile.getAbsolutePath()")
 *     .ok()
 *     .lineCount(8);
 *
 */
public abstract class CliTestBase extends ParquetFileTest {
  private final CliHarness harness = new CliHarness();

  protected CliResult cli(Object... args) throws Exception {
    String[] a = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      a[i] = String.valueOf(args[i]);
    }
    return harness.run(a);
  }

  protected CliResult cli(String commandLine) throws Exception {
    String[] args = commandLine.split("\\s+");
    return cli((Object[]) args);
  }
}
