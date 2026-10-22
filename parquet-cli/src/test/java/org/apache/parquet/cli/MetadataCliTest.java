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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class MetadataCliTest extends CliTestBase {

  @Test
  void printsMetadataForPathContainingSpaces() throws Exception {
    Path input = getTempFolder().toPath().resolve("metadata input.parquet");
    Files.copy(parquetFile().toPath(), input);

    CliResult result = cli("meta", input.toString());

    assertThat(result.exitCode()).isZero();
    assertThat(result.output())
        .contains(
            "File path:  " + input,
            "Created by: parquet-mr",
            "Schema:\nmessage schema {",
            "required int32 int32_field;",
            "required fixed_len_byte_array(12) flba_field;",
            "Row group 0:  count: 10");
  }
}
