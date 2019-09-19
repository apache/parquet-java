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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class SchemaCommandTest extends ParquetFileTest {
  @Test
  public void testSchemaCommand() throws IOException {
    File file = parquetFile();
    SchemaCommand command = new SchemaCommand(createLogger());
    command.targets = Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
  }

  @Test
  public void testSchemaCommandOverwriteExistentFile() throws IOException {
    File inputFile = parquetFile();
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avsc");
    FileUtils.touch(outputFile);
    Assert.assertEquals(0, outputFile.length());
    SchemaCommand command = new SchemaCommand(createLogger());
    command.targets = Arrays.asList(inputFile.getAbsolutePath());
    command.outputPath = outputFile.getAbsolutePath();
    command.overwrite = true;
    command.setConf(new Configuration());
    Assert.assertEquals(0, command.run());
    Assert.assertTrue(0 < outputFile.length());
  }


  @Test(expected = FileAlreadyExistsException.class)
  public void testSchemaCommandOverwriteExistentFileWithoutOverwriteOption() throws IOException {
    File inputFile = parquetFile();
    File outputFile = new File(getTempFolder(), getClass().getSimpleName() + ".avsc");
    FileUtils.touch(outputFile);
    SchemaCommand command = new SchemaCommand(createLogger());
    command.targets = Arrays.asList(inputFile.getAbsolutePath());
    command.outputPath = outputFile.getAbsolutePath();
    command.setConf(new Configuration());
    command.run();
  }
}
