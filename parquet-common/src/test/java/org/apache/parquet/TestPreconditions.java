/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class TestPreconditions {

  @Test
  public void testCheckArgumentWithoutParams() {
    assertThatCode(() -> Preconditions.checkArgument(true, "Test message")).doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkArgument(false, "Test message"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Test message");
  }

  @Test
  public void testCheckArgumentWithOneParam() {
    assertThatCode(() -> Preconditions.checkArgument(true, "Test message %s", 12))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkArgument(false, "Test message %s", 12))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Test message 12");
  }

  @Test
  public void testCheckArgumentWithTwoParams() {
    assertThatCode(() -> Preconditions.checkArgument(true, "Test message %s %s", 12, null))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkArgument(false, "Test message %s %s", 12, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Test message 12 null");
  }

  @Test
  public void testCheckArgumentWithThreeParams() {
    assertThatCode(() -> Preconditions.checkArgument(true, "Test message %s %s %s", 12, null, "column"))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkArgument(false, "Test message %s %s %s", 12, null, "column"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Test message 12 null column");
  }

  @Test
  public void testCheckArgumentWithMoreThanThreeParams() {
    assertThatCode(() -> Preconditions.checkArgument(true, "Test message %s %s %s %s", 12, null, "column", true))
        .doesNotThrowAnyException();

    assertThatThrownBy(
            () -> Preconditions.checkArgument(false, "Test message %s %s %s %s", 12, null, "column", true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Test message 12 null column true");
  }

  @Test
  public void checkArgumentMessageOnlySupportsStringTypeTemplate() {
    assertThatCode(() -> Preconditions.checkArgument(true, "Test message %d", 12))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkArgument(false, "Test message %d", 12))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("d != java.lang.String");
  }

  @Test
  public void testCheckState() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message: %s %s", 12, null))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message: %s %s", 12, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Test message: 12 null");
  }

  @Test
  public void testCheckStateWithoutArguments() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message")).doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Test message");
  }

  @Test
  public void testCheckStateWithOneArgument() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message %s", 12))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message %s", 12))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Test message 12");
  }

  @Test
  public void testCheckStateWithTwoArguments() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message %s %s", 12, null))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message %s %s", 12, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Test message 12 null");
  }

  @Test
  public void testCheckStateWithThreeArguments() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message %s %s %s", 12, null, "column"))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message %s %s %s", 12, null, "column"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Test message 12 null column");
  }

  @Test
  public void testCheckStateWithMoreThanThreeParams() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message %s %s %s %s", 12, null, "column", true))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message %s %s %s %s", 12, null, "column", true))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Test message 12 null column true");
  }

  @Test
  public void checkStateMessageOnlySupportsStringTypeTemplate() {
    assertThatCode(() -> Preconditions.checkState(true, "Test message %d", 12))
        .doesNotThrowAnyException();

    assertThatThrownBy(() -> Preconditions.checkState(false, "Test message %d", 12))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("d != java.lang.String");
  }
}
