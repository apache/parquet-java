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

import java.io.File;
import org.apache.parquet.cli.testing.CliTestBase;
import org.junit.Test;

public class ShowSizeStatisticsCliTest extends CliTestBase {

  @Test
  public void showSizeStatistics() throws Exception {
    File file = parquetFile();

    cli("size-stats " + file.getAbsolutePath())
        .ok()
        .matchOutputFromFile("src/test/resources/cli-outputs/size-stats.txt");
  }

  @Test
  public void showsHelpMessage() throws Exception {
    cli("help size-stats").ok().matchOutputFromFile("src/test/resources/cli-outputs/size-stats-help.txt");
  }

  @Test
  public void showsSchemaOutput() throws Exception {
    File file = parquetFile();
    cli("schema " + file.getAbsolutePath())
        .ok()
        .matchOutputFromFile("src/test/resources/cli-outputs/size-stats-column.txt");
  }
}
