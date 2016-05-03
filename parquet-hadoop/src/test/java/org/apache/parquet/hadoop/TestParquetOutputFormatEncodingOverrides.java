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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestParquetOutputFormatEncodingOverrides {

  @Test
  public void testEmptyOverrideString() {
    Configuration conf = new Configuration();
    Map<PrimitiveTypeName, List<Encoding>> encodings = ParquetOutputFormat.getEncodingOverrides(conf);
    assertEquals("Incorrect number of encoding entries", PrimitiveTypeName.values().length, encodings.size());

    for(Map.Entry<PrimitiveTypeName, List<Encoding>> entry : encodings.entrySet()) {
      assertTrue("Non-empty encoding list for: " + entry.getKey(), entry.getValue().isEmpty());
    }
  }

  @Test
  public void testOneValidEncoding() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_ENCODING_OVERRIDE_PREFIX + PrimitiveTypeName.BOOLEAN.name().toLowerCase(), Encoding.PLAIN.name());

    Map<PrimitiveTypeName, List<Encoding>> encodings = ParquetOutputFormat.getEncodingOverrides(conf);
    List<Encoding> encodingList = encodings.get(PrimitiveTypeName.BOOLEAN);
    assertTrue(encodingList.size() == 1);
    assertEquals(Encoding.PLAIN, encodingList.get(0));
  }

  @Test(expected = BadConfigurationException.class)
  public void testOneBrokenEncoding() {
    Configuration conf = new Configuration();
    conf.set(ParquetOutputFormat.WRITER_ENCODING_OVERRIDE_PREFIX + PrimitiveTypeName.BOOLEAN.name().toLowerCase(), "foo-encoding");

    // should fail due to an exception
    ParquetOutputFormat.getEncodingOverrides(conf);
  }

  @Test(expected = BadConfigurationException.class)
  public void testOneGood_OneBrokenEncoding() {
    Configuration conf = new Configuration();
    String encodingStr = Encoding.PLAIN.name() + "," + "foo-encoding";
    conf.set(ParquetOutputFormat.WRITER_ENCODING_OVERRIDE_PREFIX + PrimitiveTypeName.BOOLEAN.name().toLowerCase(), encodingStr);

    // should fail due to an exception
    ParquetOutputFormat.getEncodingOverrides(conf);
  }

  @Test
  public void testTwoEncodings() {
    Configuration conf = new Configuration();
    String encodingStr = Encoding.PLAIN_DICTIONARY.name() + "," + Encoding.PLAIN;
    conf.set(ParquetOutputFormat.WRITER_ENCODING_OVERRIDE_PREFIX + PrimitiveTypeName.BOOLEAN.name().toLowerCase(), encodingStr);

    Map<PrimitiveTypeName, List<Encoding>> encodings = ParquetOutputFormat.getEncodingOverrides(conf);
    List<Encoding> encodingList = encodings.get(PrimitiveTypeName.BOOLEAN);
    assertTrue(encodingList.size() == 2);
    assertEquals(Encoding.PLAIN_DICTIONARY, encodingList.get(0));
    assertEquals(Encoding.PLAIN, encodingList.get(1));
  }

  @Test(expected = BadConfigurationException.class)
  public void testThreeEncodings() {
    Configuration conf = new Configuration();
    String encodingStr = Encoding.PLAIN_DICTIONARY.name() + "," + Encoding.PLAIN + "," + Encoding.DELTA_BINARY_PACKED;
    conf.set(ParquetOutputFormat.WRITER_ENCODING_OVERRIDE_PREFIX + PrimitiveTypeName.BOOLEAN.name().toLowerCase(), encodingStr);

    // should fail due to an exception
    ParquetOutputFormat.getEncodingOverrides(conf);
  }
}
