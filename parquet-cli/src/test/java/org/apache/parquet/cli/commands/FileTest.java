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
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import com.google.common.io.Resources;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.apache.parquet.Version;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.LoggingEvent;
import org.slf4j.event.SubstituteLoggingEvent;
import org.slf4j.helpers.SubstituteLoggerFactory;

import static org.junit.Assert.assertEquals;

public abstract class FileTest {
  static final String MASK = "*****";

  static final String INT32_FIELD = "int32_field";
  static final String INT64_FIELD = "int64_field";
  static final String FLOAT_FIELD = "float_field";
  static final String DOUBLE_FIELD = "double_field";
  static final String BINARY_FIELD = "binary_field";
  static final String FIXED_LEN_BYTE_ARRAY_FIELD = "flba_field";
  static final String DATE_FIELD = "date_field";

  static final String[] COLORS = {"RED", "BLUE", "YELLOW", "GREEN", "WHITE"};

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  protected File getTempFolder() {
    return this.tempFolder.getRoot();
  }

  protected static Logger createLogger() {
    PropertyConfigurator.configure(ParquetFileTest.class.getResource("/cli-logging.properties"));
    Logger console = LoggerFactory.getLogger(ParquetFileTest.class);
    LogFactory.getFactory()
        .setAttribute("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Log4JLogger");
    return console;
  }

  @FunctionalInterface
  public interface ThrowableBiConsumer<T, U> {
    void accept(T t, U u) throws Exception;
  }

  protected static void withLogger(ThrowableBiConsumer<Logger, Queue<? extends LoggingEvent>> body) {
    SubstituteLoggerFactory loggerFactory = new SubstituteLoggerFactory();
    LinkedBlockingQueue<SubstituteLoggingEvent> loggingEvents = loggerFactory.getEventQueue();
    Logger console = loggerFactory.getLogger(ParquetFileTest.class.getName());
    try {
      body.accept(console, loggingEvents);
    } catch (RuntimeException rethrow) {
      throw rethrow;
    } catch (Exception checkedException) {
      throw new RuntimeException(checkedException);
    } finally {
      loggerFactory.clear();
    }
  }

  protected void checkOutput(String expectedFile, String actual) throws IOException {
    String original = Resources.toString(Resources.getResource(expectedFile), StandardCharsets.UTF_8);
    String expected = StringUtils.strip(original);
    assertEquals(
        expected.replace(Version.FULL_VERSION, MASK),
        actual.replace(Version.FULL_VERSION, MASK));
  }
}
