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
package org.apache.parquet.cli.testing;

import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

// CapturingLogger is a wrapper around the slf4j logger to capture CLI ourput to use with tests.
final class CapturingLogger extends MarkerIgnoringBase implements org.slf4j.Logger {
  private final StringBuilder buf = new StringBuilder();

  @Override
  public String getName() {
    return "CliTestLogger";
  }

  private void append(String msg) {
    if (msg != null && !msg.isEmpty()) {
      buf.append(msg).append('\n');
    }
  }

  private void log(String fmt, Object... args) {
    String message = MessageFormatter.arrayFormat(fmt, args).getMessage();
    append(message);
  }

  String dump() {
    return buf.toString();
  }


  // Since the CLI logic can call any console method, this is some needed delegator code to
  // ensure all methods are coverted and that the test harness does not miss anything.
  // Unfortunately slf4j API does not make this easy to do in a generic way, so we
  // have to manually add each method.

  @Override
  public boolean isTraceEnabled() {
    return true;
  }

  @Override
  public boolean isDebugEnabled() {
    return true;
  }

  @Override
  public boolean isInfoEnabled() {
    return true;
  }

  @Override
  public boolean isWarnEnabled() {
    return true;
  }

  @Override
  public boolean isErrorEnabled() {
    return true;
  }

  @Override
  public void trace(String msg) {
    append(msg);
  }

  @Override
  public void trace(String format, Object arg) {
    log(format, arg);
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    log(format, arg1, arg2);
  }

  @Override
  public void trace(String format, Object... arguments) {
    log(format, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    append(msg);
  }

  @Override
  public void debug(String msg) {
    append(msg);
  }

  @Override
  public void debug(String format, Object arg) {
    log(format, arg);
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    log(format, arg1, arg2);
  }

  @Override
  public void debug(String format, Object... arguments) {
    log(format, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    append(msg);
  }

  @Override
  public void info(String msg) {
    append(msg);
  }

  @Override
  public void info(String format, Object arg) {
    log(format, arg);
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    log(format, arg1, arg2);
  }

  @Override
  public void info(String format, Object... arguments) {
    log(format, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    append(msg);
  }

  @Override
  public void warn(String msg) {
    append(msg);
  }

  @Override
  public void warn(String format, Object arg) {
    log(format, arg);
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    log(format, arg1, arg2);
  }

  @Override
  public void warn(String format, Object... arguments) {
    log(format, arguments);
  }

  @Override
  public void warn(String msg, Throwable t) {
    append(msg);
  }

  @Override
  public void error(String msg) {
    append(msg);
  }

  @Override
  public void error(String format, Object arg) {
    log(format, arg);
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    log(format, arg1, arg2);
  }

  @Override
  public void error(String format, Object... arguments) {
    log(format, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    append(msg);
  }
}
