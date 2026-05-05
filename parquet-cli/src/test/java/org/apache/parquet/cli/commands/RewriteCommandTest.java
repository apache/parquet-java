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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.Assert;
import org.junit.Test;

public class RewriteCommandTest extends ParquetFileTest {
  @Test
  public void testRewriteCommand() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted.parquet");
    command.output = output.getAbsolutePath();
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
    Assert.assertTrue(output.exists());
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void testRewriteCommandWithoutOverwrite() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted.parquet");
    command.output = output.getAbsolutePath();
    command.setConf(new Configuration());

    Files.createFile(output.toPath());
    command.run();
  }

  @Test
  public void testRewriteCommandWithOverwrite() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted.parquet");
    command.output = output.getAbsolutePath();
    command.overwrite = true;
    command.setConf(new Configuration());

    Files.createFile(output.toPath());
    Assert.assertEquals(0, command.run());
    Assert.assertTrue(output.exists());
  }

  @Test
  public void testRewriteCommandWithCompression_GZIP() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted-1.GZIP.parquet");
    command.output = output.getAbsolutePath();
    command.codec = "GZIP";
    command.setConf(new Configuration());

    Assert.assertEquals(0, command.run());
    Assert.assertTrue(output.exists());
  }

  @Test
  public void testRewriteCommandWithCompression_gzip() throws IOException {
    File file = parquetFile();
    RewriteCommand command = new RewriteCommand(createLogger());
    command.inputs = Arrays.asList(file.getAbsolutePath());
    File output = new File(getTempFolder(), "converted-2.gzip.parquet");
    command.output = output.getAbsolutePath();
    command.codec = "gzip";
    command.setConf(new Configuration());

    Assert.assertEquals(0, command.run());
    Assert.assertTrue(output.exists());
  }
}
