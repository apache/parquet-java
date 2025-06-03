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

import java.util.Queue;
import org.apache.parquet.Version;
import org.apache.parquet.cli.ShowVersionCommand;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.event.LoggingEvent;

public class ShowVersionCommandTest extends FileTest {

  @Test
  public void testVersionCommand() {
    withLogger(this::testVersionCommand0);
  }

  private void testVersionCommand0(Logger console, Queue<? extends LoggingEvent> loggingEvents) {
    ShowVersionCommand command = new ShowVersionCommand(console);
    Assert.assertEquals(0, command.run());
    Assert.assertEquals(1, loggingEvents.size());
    LoggingEvent loggingEvent = loggingEvents.remove();
    Assert.assertEquals(Version.FULL_VERSION, loggingEvent.getMessage());
    loggingEvents.clear();
  }
}
