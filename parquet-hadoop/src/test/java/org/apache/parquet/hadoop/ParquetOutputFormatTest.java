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
package org.apache.parquet.hadoop;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class ParquetOutputFormatTest {

  private ParquetOutputFormat<?> parquetOutputFormat;
  private TaskAttemptContext context;
  private Configuration configuration;

  @Before
  public void setUp() {
    parquetOutputFormat = new ParquetOutputFormat<>();
    context = mock(TaskAttemptContext.class);
    configuration = new Configuration();
    when(context.getConfiguration()).thenReturn(configuration);
  }

  private static class MockOutputCommitter extends FileOutputCommitter {
    public MockOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
      super(outputPath, context);
    }
  }

  /* Using reflection to get around Hadoop version differences */
  private Object getOutCommitterUsingReflection() throws Exception {
    return ParquetOutputFormat.class.getMethod(
      "getOutputCommitter", Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext"))
        .invoke(parquetOutputFormat, context);
  }

  @Test
  public void getOutputCommitter() throws Exception {
    configuration.set(
      ParquetOutputFormat.OUTPUT_COMMITTER_CLASS, MockOutputCommitter.class.getName());
    assertTrue(getOutCommitterUsingReflection() instanceof MockOutputCommitter);
  }

  @Test
  public void getOutputCommitterNoSet() throws Exception {
    assertTrue(getOutCommitterUsingReflection() instanceof ParquetOutputCommitter);
  }

  @Test(expected = BadConfigurationException.class)
  public void getOutputCommitterInvalidClass() throws Throwable {
    configuration.set(ParquetOutputFormat.OUTPUT_COMMITTER_CLASS, "java.lang.404");
    try {
      getOutCommitterUsingReflection();
    } catch (final InvocationTargetException ex) {
      throw ex.getCause();
    }
  }
}
