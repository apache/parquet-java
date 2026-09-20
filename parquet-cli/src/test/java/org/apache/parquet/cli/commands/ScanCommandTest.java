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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Queue;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.event.LoggingEvent;

public class ScanCommandTest extends ParquetFileTest {
  @Test
  public void testScanCommand() throws IOException {
    File file = parquetFile();
    ScanCommand command = new ScanCommand(createLogger());
    command.sourceFiles = Arrays.asList(file.getAbsolutePath());
    command.setConf(new Configuration());
    assertThat(command.run()).isZero();
  }

  @Test
  public void testScanCommandWithMultipleSourceFiles() throws IOException {
    File file = parquetFile();
    ScanCommand command = new ScanCommand(createLogger());
    command.sourceFiles = Arrays.asList(file.getAbsolutePath(), file.getAbsolutePath());
    command.setConf(new Configuration());
    assertThat(command.run()).isZero();
  }

  @Test
  public void testScanCommandWithSpecificColumns() {
    withLogger(this::testScanCommandWithSpecificColumns);
  }

  private void testScanCommandWithSpecificColumns(Logger console, Queue<? extends LoggingEvent> loggingEvents)
      throws IOException {
    File file = parquetFile();
    ScanCommand command = new ScanCommand(console);
    command.sourceFiles = Arrays.asList(file.getAbsolutePath());
    command.columns = Arrays.asList(INT32_FIELD, INT64_FIELD);
    command.setConf(new Configuration());

    assertThat(command.run()).isZero();
    assertThat(loggingEvents).extracting(LoggingEvent::getMessage).contains("Scanned 10 records from 1 file(s)");
    loggingEvents.clear();
  }

  @Test
  public void testScanCommandWithInvalidColumnName() {
    File file = parquetFile();
    ScanCommand command = new ScanCommand(createLogger());
    command.sourceFiles = Arrays.asList(file.getAbsolutePath());
    command.columns = Arrays.asList("invalid_field");
    command.setConf(new Configuration());
    assertThatThrownBy(command::run)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find field 'invalid_field' in schema");
  }

  @Test
  public void testScanCommandValidatesAllFilesBeforeScanning() {
    withLogger(this::testScanCommandValidatesAllFilesBeforeScanning);
  }

  private void testScanCommandValidatesAllFilesBeforeScanning(
      Logger console, Queue<? extends LoggingEvent> loggingEvents) throws IOException {
    File validFile = parquetFile();
    File invalidFile = AvroIsolationParquetFiles.writeNestedInt96(
        new File(getTempFolder(), "scan_invalid_projection_nested_int96.parquet"));
    ScanCommand command = new ScanCommand(console);
    command.sourceFiles = Arrays.asList(validFile.getAbsolutePath(), invalidFile.getAbsolutePath());
    command.columns = Arrays.asList(INT32_FIELD);
    command.setConf(new Configuration());

    assertThatThrownBy(command::run)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find field '" + INT32_FIELD + "' in schema");
    assertThat(loggingEvents).isEmpty();
  }

  @Test
  public void testScanCommandWithAvroCompatListInList() throws IOException {
    File listInListFile = AvroIsolationParquetFiles.writeAvroCompatListInList(
        new File(getTempFolder(), "scan_avro_compat_list_in_list.parquet"));

    ScanCommand command = new ScanCommand(createLogger());
    command.sourceFiles = Arrays.asList(listInListFile.getAbsolutePath());
    command.setConf(new Configuration());

    assertThat(command.run()).isZero();
  }

  @Test
  public void testScanCommandWithAvroCompatListInListProjection() throws IOException {
    File listInListFile = AvroIsolationParquetFiles.writeAvroCompatListInList(
        new File(getTempFolder(), "scan_avro_compat_list_in_list_projected.parquet"));

    ScanCommand command = new ScanCommand(createLogger());
    command.sourceFiles = Arrays.asList(listInListFile.getAbsolutePath());
    command.columns = Arrays.asList(AvroIsolationParquetFiles.LIST_OF_LISTS);
    command.setConf(new Configuration());

    assertThat(command.run()).isZero();
  }

  @Test
  public void testScanCommandWithNestedInt96() throws IOException {
    File int96File =
        AvroIsolationParquetFiles.writeNestedInt96(new File(getTempFolder(), "scan_nested_int96.parquet"));

    ScanCommand command = new ScanCommand(createLogger());
    command.sourceFiles = Arrays.asList(int96File.getAbsolutePath());
    command.setConf(new Configuration());

    assertThat(command.run()).isZero();
  }
}
