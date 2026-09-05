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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

public class TestPforConfiguration {

  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType("message m { required int32 i; required double d; }");

  private static ColumnDescriptor intColumn() {
    return SCHEMA.getColumns().get(0);
  }

  private static ColumnDescriptor doubleColumn() {
    return SCHEMA.getColumns().get(1);
  }

  @Test
  public void testDefault() throws Exception {
    Configuration conf = new Configuration();
    // PFOR is off unless a job asks for it, and the delta mode is on wherever PFOR is
    assertEquals(ParquetProperties.DEFAULT_IS_PFOR_ENABLED, ParquetOutputFormat.getPforEnabled(conf));
    assertEquals(ParquetProperties.DEFAULT_IS_PFOR_DELTA_ENABLED, ParquetOutputFormat.getPforDeltaEnabled(conf));
  }

  @Test
  public void testTheKeyNamesAreTheDocumentedOnes() throws Exception {
    // These strings are the public surface; the class javadoc documents them
    assertEquals("parquet.enable.pfor", ParquetOutputFormat.ENABLE_PFOR);
    assertEquals("parquet.enable.pfor.delta", ParquetOutputFormat.ENABLE_PFOR_DELTA);
  }

  @Test
  public void testSetTrue() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR, true);
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR_DELTA, true);
    assertTrue(ParquetOutputFormat.getPforEnabled(conf));
    assertTrue(ParquetOutputFormat.getPforDeltaEnabled(conf));
  }

  @Test
  public void testSetFalse() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR, false);
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR_DELTA, false);
    assertFalse(ParquetOutputFormat.getPforEnabled(conf));
    assertFalse(ParquetOutputFormat.getPforDeltaEnabled(conf));
  }

  @Test
  public void testTheDeltaModeCanBeTurnedOffWhilePforStaysOn() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR, true);
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR_DELTA, false);
    assertTrue(ParquetOutputFormat.getPforEnabled(conf));
    assertFalse(ParquetOutputFormat.getPforDeltaEnabled(conf));
  }

  @Test
  public void testTheKeysReachTheWriterProperties() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR, true);
    conf.setBoolean(ParquetOutputFormat.ENABLE_PFOR_DELTA, false);

    ParquetProperties props = ParquetProperties.builder()
        .withPforEncoding(ParquetOutputFormat.getPforEnabled(conf))
        .withPforDeltaEncoding(ParquetOutputFormat.getPforDeltaEnabled(conf))
        .build();

    assertTrue(props.isPforEnabled(intColumn()));
    assertFalse(props.isPforDeltaEnabled(intColumn()));
    // PFOR encodes INT32 and INT64 only, whatever the configuration says
    assertFalse(props.isPforEnabled(doubleColumn()));
  }
}
