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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ShowFooterCommandTest extends ParquetFileTest {
  @Test
  public void testShowDirectoryCommand() throws IOException {
    File file = parquetFile();
    ShowFooterCommand command = new ShowFooterCommand(createLogger());
    command.target = file.getAbsolutePath();
    command.raw = false;
    command.setConf(new Configuration());
    assertEquals(0, command.run());

    command.raw = true;
    assertEquals(0, command.run());
  }
}
