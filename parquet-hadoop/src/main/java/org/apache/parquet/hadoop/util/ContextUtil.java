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
package org.apache.parquet.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.counters.GenericCounter;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * Utility methods for creating and accessing Hadoop MapReduce contexts.
 * Runtime exceptions from Hadoop constructors and methods propagate directly to callers.
 *
 * @deprecated Use {@link JobContextImpl}, {@link TaskAttemptContextImpl}, and {@link GenericCounter}
 * constructors and Hadoop context and counter methods directly. All supported contexts expose counter
 * methods; check for a non-null context instead of using {@link #hasCounterMethod(TaskAttemptContext)}.
 */
@Deprecated
public class ContextUtil {

  /**
   * Creates a JobContext from a configuration and job ID. {@code jobId} may be null.
   *
   * @param conf  a configuration
   * @param jobId a job id
   * @return a job context
   */
  public static JobContext newJobContext(Configuration conf, JobID jobId) {
    return new JobContextImpl(conf, jobId);
  }

  /**
   * Creates a TaskAttemptContext from a configuration and task attempt ID.
   *
   * @param conf          a configuration
   * @param taskAttemptId a task attempt id
   * @return a task attempt context
   */
  public static TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID taskAttemptId) {
    return new TaskAttemptContextImpl(conf, taskAttemptId);
  }

  /**
   * @param name        a string name
   * @param displayName a string display name
   * @param value       an initial value
   * @return a counter with the given name, display name and initial value
   */
  public static Counter newGenericCounter(String name, String displayName, long value) {
    return new GenericCounter(name, displayName, value);
  }

  /**
   * Returns the configuration of a JobContext.
   *
   * @param context a job context
   * @return the context's configuration
   */
  public static Configuration getConfiguration(JobContext context) {
    return context.getConfiguration();
  }

  public static Counter getCounter(TaskAttemptContext context, String groupName, String counterName) {
    return context.getCounter(groupName, counterName);
  }

  /**
   * Returns whether a context is available for counter access. All supported
   * TaskAttemptContext implementations expose counter methods.
   *
   * @param context a task attempt context, or null
   * @return true if the context is non-null
   */
  public static boolean hasCounterMethod(TaskAttemptContext context) {
    return context != null;
  }

  public static void incrementCounter(Counter counter, long increment) {
    counter.increment(increment);
  }
}
