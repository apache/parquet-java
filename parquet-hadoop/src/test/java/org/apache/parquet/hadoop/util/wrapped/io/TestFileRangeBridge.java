/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.parquet.hadoop.util.wrapped.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThatCode;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link FileRangeBridge} class; requires
 * Hadoop 3.3.5+
 */
public class TestFileRangeBridge {

  private static final Logger LOG = LoggerFactory.getLogger(TestFileRangeBridge.class);

  /**
   * Class required for tests to run: {@value }.
   * Probed in setup.
   */
  static final String CLASSNAME = "org.apache.hadoop.fs.FileRange";

  @Before
  public void setUp() {
    assumeThatCode(() -> this.getClass().getClassLoader().loadClass(CLASSNAME))
        .doesNotThrowAnyException();
  }

  /**
   * Attempt to explicit instantiate the bridge;
   * package private constructor.
   */
  @Test
  public void testInstantiate() throws Throwable {
    new FileRangeBridge();
  }

  /**
   * Is the shared bridge instance non-null, and does the
   * {@link FileRangeBridge#bridgeAvailable()} predicate
   * return true.
   */
  @Test
  public void testFileRangeBridgeAvailable() throws Throwable {
    assertThat(FileRangeBridge.instance())
        .as("FileRangeBridge instance null")
        .isNotNull();
    assertThat(FileRangeBridge.bridgeAvailable()).as("Bridge not available").isTrue();
  }

  /**
   * Create a range and validate the getters return the original values.
   */
  @Test
  public void testCreateFileRange() {
    Object reference = "backref";
    FileRangeBridge.WrappedFileRange range = FileRangeBridge.instance().createFileRange(512L, 16384, reference);
    LOG.info("created range {}", range);
    assertThat(range).as("null range").isNotNull();
    assertThat(range.getFileRange()).as("null range instance").isNotNull();
    assertThat(range.getOffset()).as("offset of " + range).isEqualTo(512L);
    assertThat(range.getLength()).as("length of " + range).isEqualTo(16384);
    assertThat(range.getReference()).as("backref of " + range).isSameAs(reference);

    // this isn't set until readVectored() is called
    assertThat(range.getData()).as("non-null range future").isNull();
  }

  /**
   * Verify there is no parameter validation.
   */
  @Test
  public void testCreateInvalidRange() {
    FileRangeBridge.instance().createFileRange(-1, -1, null);
  }
}
