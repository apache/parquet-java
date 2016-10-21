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
import java.io.IOException;
import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.PosixParser;

import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import org.apache.parquet.tools.Main;

public class TestHeadCommand {
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

  @Test(expected = IOException.class)
  public void testHeadFileWrongPath() throws Exception {
    // test for cat on invalid Parquet file
    HeadCommand command = new HeadCommand();
    String path = getClass().getResource("/org/apache/parquet/tools/build/readme").toString();
    CommandLine cmd = getCommandLine(command, new String[]{path});
    command.execute(cmd);
  }

  @Test
  public void testHeadFile() throws Exception {
    // test to read individual file with default formatter
    HeadCommand command = new HeadCommand();
    String file = "/org/apache/parquet/tools/build/sample.parquet/" +
      "part-r-00000-d9a01963-c804-48f8-ad7c-41ec3178910e.gz.parquet";
    Path path = new Path(getClass().getResource(file).toURI());
    CommandLine cmd = getCommandLine(command, new String[]{path.toString()});
    command.execute(cmd);
    assertEquals("id = 1\nstr = str\nflag = true", stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testHeadFileJson() throws Exception {
    // test to read individual file as JSON
    HeadCommand command = new HeadCommand();
    String file = "/org/apache/parquet/tools/build/sample.parquet/" +
      "part-r-00000-d9a01963-c804-48f8-ad7c-41ec3178910e.gz.parquet";
    Path path = new Path(getClass().getResource(file).toURI());
    CommandLine cmd = getCommandLine(command, new String[]{"-j", path.toString()});
    command.execute(cmd);
    assertEquals("{\"id\":1,\"str\":\"str\",\"flag\":true}", stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testHeadDirectory() throws Exception {
    // test to read directory of Parquet files
    HeadCommand command = new HeadCommand();
    String file = "/org/apache/parquet/tools/build/sample.parquet/_SUCCESS";
    Path path = new Path(getClass().getResource(file).toURI()).getParent();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", path.toString()});
    command.execute(cmd);
    String expected =
      "{\"id\":1,\"str\":\"str\",\"flag\":true}\n" +
      "{\"id\":2,\"str\":\"str\",\"flag\":true}\n" +
      "{\"id\":3,\"str\":\"str\",\"flag\":true}\n" +
      "{\"id\":4,\"str\":\"str\",\"flag\":true}";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }

  @Test
  public void testHeadDirectoryWithLimit() throws Exception {
    // test to read certain number of records from directory of Parquet files
    HeadCommand command = new HeadCommand();
    String file = "/org/apache/parquet/tools/build/sample.parquet/_SUCCESS";
    Path path = new Path(getClass().getResource(file).toURI()).getParent();
    CommandLine cmd = getCommandLine(command, new String[]{"-j", "-n", "2", path.toString()});
    command.execute(cmd);
    String expected =
      "{\"id\":1,\"str\":\"str\",\"flag\":true}\n" +
      "{\"id\":2,\"str\":\"str\",\"flag\":true}";
    assertEquals(expected, stdout.toString().trim());
    assertEquals("", stderr.toString().trim());
  }
}
