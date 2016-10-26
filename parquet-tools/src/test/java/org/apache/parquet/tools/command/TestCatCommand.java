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

public class TestCatCommand {
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
  public void testCatFileWrongPath1() throws Exception {
    // test fails to initialize Path from empty string
    CatCommand command = new CatCommand();
    CommandLine cmd = getCommandLine(command, new String[]{""});
    command.execute(cmd);
  }

  @Test(expected = IOException.class)
  public void testCatFileWrongPath2() throws Exception {
    // test for non-existent path
    File file = temp.newFile();
    assertTrue(file.delete());
    CatCommand command = new CatCommand();
    CommandLine cmd = getCommandLine(command, new String[]{file.toString()});
    command.execute(cmd);
  }

  @Test(expected = IOException.class)
  public void testCatFileWrongPath3() throws Exception {
    // test for cat on invalid Parquet file
    File file = temp.newFile();
    CatCommand command = new CatCommand();
    CommandLine cmd = getCommandLine(command, new String[]{file.toString()});
    command.execute(cmd);
  }

  @Test
  public void testCatFile() throws Exception {
    // test to read individual file with default formatter
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "sample.parquet");
    ParquetToolsWrite.writeParquetFile(path, 4);

    CatCommand command = new CatCommand();
    CommandLine cmd = getCommandLine(command, new String[]{path.toString()});
    command.execute(cmd);
    String expected =
      "id = 0\nstr = id-0\nflag = true\n\n" +
      "id = 1\nstr = id-1\nflag = false\n\n" +
      "id = 2\nstr = id-2\nflag = true\n\n" +
      "id = 3\nstr = id-3\nflag = false";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testCatFileJson() throws Exception {
    // test to read individual file as JSON
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "sample.parquet");
    ParquetToolsWrite.writeParquetFile(path, 4);

    CatCommand command = new CatCommand();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", path.toString()});
    command.execute(cmd);
    String expected =
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}\n" +
      "{\"id\":2,\"str\":\"id-2\",\"flag\":true}\n" +
      "{\"id\":3,\"str\":\"id-3\",\"flag\":false}";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testCatDirectory() throws Exception {
    // test to read directory of Parquet files
    File file = temp.newFolder();
    Path path = new Path(file.toString()).suffix(Path.SEPARATOR + "table.parquet");
    ParquetToolsWrite.writeParquetTable(path, 3, 3, true);

    CatCommand command = new CatCommand();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", path.toString()});
    command.execute(cmd);
    String expected =
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}\n" +
      "{\"id\":2,\"str\":\"id-2\",\"flag\":true}\n" +
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}\n" +
      "{\"id\":2,\"str\":\"id-2\",\"flag\":true}\n" +
      "{\"id\":0,\"str\":\"id-0\",\"flag\":true}\n" +
      "{\"id\":1,\"str\":\"id-1\",\"flag\":false}\n" +
      "{\"id\":2,\"str\":\"id-2\",\"flag\":true}";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }
}
