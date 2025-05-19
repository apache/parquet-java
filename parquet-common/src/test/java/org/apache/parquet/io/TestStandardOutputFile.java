/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.parquet.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class TestStandardOutputFile {

  private static final String TEST = "this is a test";

  @Test
  public void outputFileWriteByteToStdOut() throws IOException, InterruptedException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    final PositionOutputStream stdOut = new StandardOutputFile().create(1);
    final byte test = 7;
    stdOut.write(test);
    assertEquals(1, stdOut.getPos());
    assertEquals(1, baos.toByteArray().length);
    assertEquals(7, baos.toByteArray()[0]);
  }

  @Test
  public void outputFileWriteArrayToStdOut() throws IOException, InterruptedException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    final PositionOutputStream stdOut = new StandardOutputFile().create(1);
    final byte[] test = TEST.getBytes();
    stdOut.write(test);
    assertEquals(test.length, stdOut.getPos());
    assertEquals(test.length, baos.toByteArray().length);
    assertEquals(TEST, new String(baos.toByteArray()));
  }

  @Test
  public void outputFileWriteArrayOffsetToStdOut() throws IOException, InterruptedException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    final PositionOutputStream stdOut = new StandardOutputFile().create(1);
    final byte[] test = TEST.getBytes();
    stdOut.write(test, 1, 4);
    assertEquals(4, stdOut.getPos());
    assertEquals(4, baos.toByteArray().length);
    assertEquals(TEST.substring(1, 5), new String(baos.toByteArray()));
  }

}
