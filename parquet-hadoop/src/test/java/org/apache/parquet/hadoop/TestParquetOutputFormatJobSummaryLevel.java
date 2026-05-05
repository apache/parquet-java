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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel;
import org.junit.Test;

public class TestParquetOutputFormatJobSummaryLevel {
  @Test
  public void testDefault() throws Exception {
    Configuration conf = new Configuration();
    // default should be ALL
    assertEquals(JobSummaryLevel.ALL, ParquetOutputFormat.getJobSummaryLevel(conf));
  }

  @Test
  public void testDeprecatedStillWorks() throws Exception {
    Configuration conf = new Configuration();

    conf.set(ParquetOutputFormat.ENABLE_JOB_SUMMARY, "true");
    assertEquals(JobSummaryLevel.ALL, ParquetOutputFormat.getJobSummaryLevel(conf));

    conf.set(ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false");
    assertEquals(JobSummaryLevel.NONE, ParquetOutputFormat.getJobSummaryLevel(conf));
  }

  @Test
  public void testLevelParses() throws Exception {
    Configuration conf = new Configuration();

    conf.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, "all");
    assertEquals(JobSummaryLevel.ALL, ParquetOutputFormat.getJobSummaryLevel(conf));

    conf.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, "common_only");
    assertEquals(JobSummaryLevel.COMMON_ONLY, ParquetOutputFormat.getJobSummaryLevel(conf));

    conf.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, "none");
    assertEquals(JobSummaryLevel.NONE, ParquetOutputFormat.getJobSummaryLevel(conf));
  }

  @Test
  public void testLevelTakesPrecedence() throws Exception {
    Configuration conf = new Configuration();

    conf.set(ParquetOutputFormat.JOB_SUMMARY_LEVEL, "common_only");
    conf.set(ParquetOutputFormat.ENABLE_JOB_SUMMARY, "false");
    assertEquals(JobSummaryLevel.COMMON_ONLY, ParquetOutputFormat.getJobSummaryLevel(conf));
  }
}
