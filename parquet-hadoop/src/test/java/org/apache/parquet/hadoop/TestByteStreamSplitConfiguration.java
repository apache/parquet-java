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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.junit.Test;

public class TestByteStreamSplitConfiguration {
  @Test
  public void testDefault() throws Exception {
    Configuration conf = new Configuration();
    // default should be false
    assertEquals(
        ParquetProperties.DEFAULT_IS_BYTE_STREAM_SPLIT_ENABLED,
        ParquetOutputFormat.getByteStreamSplitEnabled(conf));
  }

  @Test
  public void testSetTrue() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.ENABLE_BYTE_STREAM_SPLIT, true);
    assertTrue(ParquetOutputFormat.getByteStreamSplitEnabled(conf));
  }

  @Test
  public void testSetFalse() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.ENABLE_BYTE_STREAM_SPLIT, false);
    assertFalse(ParquetOutputFormat.getByteStreamSplitEnabled(conf));
  }
}
