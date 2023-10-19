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

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class ShowDictionaryCommandTest extends ParquetFileTest {
  @Test
  public void testShowDirectoryCommand() throws IOException {
    File file = parquetFile();
    ShowDictionaryCommand command = new ShowDictionaryCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.column = BINARY_FIELD;
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testShowDirectoryCommandWithoutDictionaryEncoding() throws IOException {
    File file = parquetFile();
    ShowDictionaryCommand command = new ShowDictionaryCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    // the 'double_field' column does not have dictionary encoding
    command.column = DOUBLE_FIELD;
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testShowDirectoryCommandForFixedLengthByteArray() throws IOException {
    File file = parquetFile();
    ShowDictionaryCommand command = new ShowDictionaryCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.column = FIXED_LEN_BYTE_ARRAY_FIELD;
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }
}
