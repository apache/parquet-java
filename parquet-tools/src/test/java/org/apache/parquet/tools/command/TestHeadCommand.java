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

package org.apache.parquet.tools.command;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.PosixParser;

import org.apache.hadoop.fs.Path;

import org.junit.Rule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.parquet.tools.Main;

public class TestHeadCommand {
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private ByteArrayOutputStream stdout = null;
  private ByteArrayOutputStream stderr = null;

  /** Get command line object based on provided arguments */
  private CommandLine getCommandLine(Command command, String[] args) throws Exception {
    CommandLineParser parser = new PosixParser();
    return parser.parse(command.getOptions(), args, command.supportsExtraArgs());
  }

  @Before
  public void setUp() {
    // set stdout and stderr from command output to be redirected into simple byte stream,
    // so we can check results
    stdout = new ByteArrayOutputStream();
    stderr = new ByteArrayOutputStream();
    Main.out = new PrintStream(stdout);
    Main.err = new PrintStream(stderr);
  }

  @After
  public void tearDown() {
    if (Main.out != null) {
      Main.out.close();
      Main.out = null;
    }

    if (Main.err != null) {
      Main.err.close();
      Main.err = null;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHeadFileWrongPath1() throws Exception {
    // test fails to initialize Path from empty string
    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{""});
    command.execute(cmd);
  }

  @Test(expected = IOException.class)
  public void testHeadFileWrongPath2() throws Exception {
    // test reading of invalid Parquet file
    File file = temp.newFile();
    assertTrue(file.delete());
    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{file.toString()});
    command.execute(cmd);
  }

  @Test(expected = IOException.class)
  public void testHeadFileWrongPath3() throws Exception {
    // test for head on invalid Parquet file
    File file = temp.newFile();
    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{file.toString()});
    command.execute(cmd);
  }

  @Test
  public void testHeadFile() throws Exception {
    // test to read individual file with default formatter
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "sample.parquet");
    ParquetToolsWrite.writeParquetFile(path, 1);

    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{path.toString()});
    command.execute(cmd);
    assertEquals("id = 0\nstr = id-0\nflag = true", stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testHeadFileJson() throws Exception {
    // test to read individual file as JSON
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "sample.parquet");
    ParquetToolsWrite.writeParquetFile(path, 1);

    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", path.toString()});
    command.execute(cmd);
    assertEquals("{\"id\":0,\"str\":\"id-0\",\"flag\":true}", stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testHeadDirectory() throws Exception {
    // test to read directory of Parquet files, by default fetches 5 records
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "table.parquet");
    ParquetToolsWrite.writeParquetTable(path, 3, 2, true);

    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", path.toString()});
    command.execute(cmd);
    String expected =
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}\n" +
      "{\"id\":2,\"str\":\"id-2\",\"flag\":true}\n" +
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testHeadDirectoryWithLimit() throws Exception {
    // test to read certain number of records from directory of Parquet files
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "table.parquet");
    ParquetToolsWrite.writeParquetTable(path, 3, 2, true);

    HeadCommand command = new HeadCommand();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", "-n", "4", path.toString()});
    command.execute(cmd);
    String expected =
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}\n" +
      "{\"id\":2,\"str\":\"id-2\",\"flag\":true}\n" +
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }
}
