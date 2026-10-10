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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.cli.commands.ParquetFileTest;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.helpers.SubstituteLoggerFactory;

abstract class CliTestBase extends ParquetFileTest {

  protected static CliResult cli(String... args) throws Exception {
    SubstituteLoggerFactory loggerFactory = new SubstituteLoggerFactory();
    try {
      int exitCode = ToolRunner.run(new Configuration(), new Main(loggerFactory.getLogger("cli-test")), args);
      String output = loggerFactory.getEventQueue().stream()
          .filter(event -> event.getLevel().toInt() >= Level.INFO.toInt())
          .map(CliTestBase::formatEvent)
          .collect(Collectors.joining("\n"));
      return new CliResult(exitCode, output);
    } finally {
      loggerFactory.clear();
    }
  }

  private static String formatEvent(LoggingEvent event) {
    String message = MessageFormatter.arrayFormat(event.getMessage(), event.getArgumentArray())
        .getMessage();
    if (event.getThrowable() == null) {
      return message;
    }
    StringWriter stackTrace = new StringWriter();
    event.getThrowable().printStackTrace(new PrintWriter(stackTrace));
    return message + "\n" + stackTrace;
  }

  protected record CliResult(int exitCode, String output) {}
}
