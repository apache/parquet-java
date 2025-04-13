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
import java.util.Queue;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.event.LoggingEvent;

public class ShowFooterCommandTest extends ParquetFileTest {

  @Test
  public void testShowDirectoryCommand() {
    withLogger(this::testShowDirectoryCommand0);
  }

  private void testShowDirectoryCommand0(
      Logger console, Queue<? extends LoggingEvent> loggingEvents) throws IOException {
    File file = parquetFile();
    ShowFooterCommand command = new ShowFooterCommand(console);
    command.target = file.getAbsolutePath();
    command.raw = false;
    command.setConf(new Configuration());
    assertEquals(0, command.run());
    assertEquals(1, loggingEvents.size());
    LoggingEvent loggingEvent = loggingEvents.remove();
    checkOutput("cli-output/show_footer_command.txt", loggingEvent.getMessage());
    loggingEvents.clear();

    command.raw = true;
    assertEquals(0, command.run());
    assertEquals(1, loggingEvents.size());
    loggingEvent = loggingEvents.remove();
    checkOutput("cli-output/show_footer_command_raw.txt", loggingEvent.getMessage());
  }
}
