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

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class CliResult {
  public final int exitCode;
  public final String text;

  CliResult(int exitCode, String text) {
    this.exitCode = exitCode;
    this.text = text;
  }

  public CliResult ok() {
    assertEquals("exit", 0, exitCode);
    return this;
  }

  public CliResult fails(int code) {
    assertEquals("exit", code, exitCode);
    return this;
  }

  public CliResult outputContains(String... parts) {
    for (String p : parts) assertTrue("missing: " + p, text.contains(p));
    return this;
  }

  public CliResult outputNotContains(String... parts) {
    for (String p : parts) assertFalse("should not contain: " + p, text.contains(p));
    return this;
  }

  public CliResult lineCount(int expected) {
    long cnt = 0;
    for (String line : text.split("\n")) {
      if (!line.trim().isEmpty()) {
        cnt++;
      }
    }
    assertEquals(expected, cnt);
    return this;
  }

  public CliResult matchOutputFromFile(String filePath) throws Exception {
    String expected = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
    return outputContains(expected);
  }
}
