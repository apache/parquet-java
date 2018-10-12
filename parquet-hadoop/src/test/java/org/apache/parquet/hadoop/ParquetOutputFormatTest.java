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
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ParquetOutputFormatTest {

  private ParquetOutputFormat<?> parquetOutputFormat;
  private TaskAttemptContext context;
  private Configuration configuration;


  @Before
  public void setUp() throws Exception {
    parquetOutputFormat = new ParquetOutputFormat<>();
    context = mock(TaskAttemptContext.class);
    configuration = new Configuration();
    when(context.getConfiguration()).thenReturn(configuration);
  }

  public static class MockOutputCommitter extends FileOutputCommitter {
    public MockOutputCommitter(
      final Path outputPath,
      final org.apache.hadoop.mapreduce.TaskAttemptContext context) throws IOException {
      super(outputPath, context);
    }
  }

  private OutputCommitter getOutCommitter() throws Exception {
    return parquetOutputFormat.getOutputCommitter(context);
  }

  @Test
  public void getOutputCommitter() throws Exception {
    configuration.set(
      ParquetOutputFormat.OUTPUT_COMMITTER_CLASS, MockOutputCommitter.class.getName());
    assertTrue(getOutCommitter() instanceof MockOutputCommitter);
  }

  @Test
  public void getOutputCommitterNoSet() throws Exception {
    assertTrue(getOutCommitter() instanceof ParquetOutputCommitter);
  }

  @Test(expected = BadConfigurationException.class)
  public void getOutputCommitterInvalidClass() throws Throwable {
    configuration.set(ParquetOutputFormat.OUTPUT_COMMITTER_CLASS, "java.lang.404");
    getOutCommitter();
  }
}
