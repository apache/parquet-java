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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class BaseCommandTest {

  private static final String WIN_FILE_PATH = "C:\\Test\\Downloads\\test.parquet";
  private static final String LINUX_FILE_PATH = "/var/tmp/test.parquet";

  private Logger console = LoggerFactory.getLogger(BaseCommandTest.class);
  private TestCommand command;

  @Before
  public void setUp() {
    this.command = new TestCommand(this.console);
  }

  @Test
  public void qualifiedPathOnLinuxTest() throws IOException {
    Path path = this.command.qualifiedPath(LINUX_FILE_PATH);
    Assert.assertEquals("test.parquet", path.getName());
  }

  @Test
  public void qualifiedPathOnWinTest() throws IOException {
    Path path = this.command.qualifiedPath(WIN_FILE_PATH);
    Assert.assertEquals("test.parquet", path.getName());
  }

  @Test
  public void qualifiedURILinuxFileTest() throws IOException {
    URI uri = this.command.qualifiedURI(LINUX_FILE_PATH);
    Assert.assertEquals("/var/tmp/test.parquet", uri.getPath());
  }

  @Test
  public void qualifiedURIWinFileTest() throws IOException {
    URI uri = this.command.qualifiedURI(WIN_FILE_PATH);
    Assert.assertEquals("/C:/Test/Downloads/test.parquet", uri.getPath());
  }

  @Test
  public void qualifiedURIResourceURITest() throws IOException {
    URI uri = this.command.qualifiedURI("resource:/a");
    Assert.assertEquals("/a", uri.getPath());
  }

  class TestCommand extends BaseCommand {

    public TestCommand(Logger console) {
      super(console);
      setConf(new Configuration());
    }

    @Override
    public int run() throws IOException {
      return 0;
    }

    @Override
    public List<String> getExamples() {
      return null;
    }
  }
}
