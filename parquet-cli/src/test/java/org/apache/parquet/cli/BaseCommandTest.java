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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseCommandTest {

  private static final String FILE_PATH = "/var/tmp/test.parquet";
  private static final String WIN_FILE_PATH = "C:\\Test\\Downloads\\test.parquet";

  private Logger console = LoggerFactory.getLogger(BaseCommandTest.class);
  private TestCommand command;

  @Before
  public void setUp() {
    this.command = new TestCommand(this.console);
  }

  // For All OS
  @Test
  public void qualifiedPathTest() throws IOException {
    Path path = this.command.qualifiedPath(FILE_PATH);
    assertThat(path.getName()).isEqualTo("test.parquet");
  }

  @Test
  public void qualifiedURITest() throws IOException {
    URI uri = this.command.qualifiedURI(FILE_PATH);
    assertThat(uri.getPath()).isEqualTo("/var/tmp/test.parquet");
  }

  @Test
  public void qualifiedURIResourceURITest() throws IOException {
    URI uri = this.command.qualifiedURI("resource:/a");
    assertThat(uri.getPath()).isEqualTo("/a");
  }

  @Test
  public void hexToBytes() {
    assertThat(this.command.hexToBytes("0x10")).containsExactly(0x10);
    assertThat(this.command.hexToBytes("0x0506")).containsExactly(0x05, 0x06);
    assertThat(this.command.hexToBytes("0506")).containsExactly(0x05, 0x06);
    assertThat(this.command.hexToBytes("0x010203")).containsExactly(0x01, 0x02, 0x03);
  }

  @Test
  public void hexToBytesRejectsInvalidHexString() {
    assertThatThrownBy(() -> this.command.hexToBytes("0x011"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid hex string: 0x011");

    assertThatThrownBy(() -> this.command.hexToBytes("0xABZZ"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid hex string: 0xABZZ");

    assertThatThrownBy(() -> this.command.hexToBytes("0xabgg"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid hex string: 0xabgg");
  }

  // For Windows
  @Test
  public void qualifiedPathTestForWindows() throws IOException {
    Assume.assumeTrue(System.getProperty("os.name").toLowerCase().startsWith("win"));
    Path path = this.command.qualifiedPath(WIN_FILE_PATH);
    assertThat(path.getName()).isEqualTo("test.parquet");
  }

  @Test
  public void qualifiedURITestForWindows() throws IOException {
    Assume.assumeTrue(System.getProperty("os.name").toLowerCase().startsWith("win"));
    URI uri = this.command.qualifiedURI(WIN_FILE_PATH);
    assertThat(uri.getPath()).isEqualTo("/C:/Test/Downloads/test.parquet");
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
