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
package org.apache.parquet.thrift;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import thrift.test.OneOfEach;

import static org.junit.Assert.assertEquals;

public class TestDictionaryEncoding {
  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private List<OneOfEach> getFixtures() throws Exception {
    List<OneOfEach> fixtures = new ArrayList<OneOfEach>();
    for (int i = 0; i < 1000; i++) {
      OneOfEach ooe = new OneOfEach();
      ooe.setIm_true(i % 2 == 0);
      ooe.setIm_false(i % 2 != 0);
      ooe.setA_bite((byte) (i % 10));
      ooe.setInteger16((short) (i % 100));
      ooe.setInteger32(i % 28);
      ooe.setInteger64(i);
      ooe.setDouble_precision(((double) i) / 2.0);
      ooe.setSome_characters("a string " + i % 10);
      ooe.setZomg_unicode("no thanks"); // test dictionary size = 1
      ooe.setWhat_who(false);
      ooe.setBase64(("zomg bytes " + (i % 10)).getBytes("UTF-8"));

      byte z = (byte)(i % 30);
      ooe.setByte_list(Arrays.asList(z, (byte)(z + 1), (byte)(z + 2), (byte)(z - 100)));
      ooe.setI16_list(Arrays.<Short>asList()); // zero length dictionary
      fixtures.add(ooe);
    }
    return fixtures;
  }

  @Test
  public void testDictionaryEncoding() throws Exception {
    Path p = new Path(new File(tempDir.getRoot(), "out").getAbsolutePath());
    ThriftParquetWriter<OneOfEach> writer = new ThriftParquetWriter<OneOfEach>(p,
        OneOfEach.class,
        CompressionCodecName.UNCOMPRESSED,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        true, // Dictionary encoding enabled = true
        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);

    List<OneOfEach> expected = getFixtures();

    for (OneOfEach ooe : expected) {
      writer.write(ooe);
    }
    writer.close();

    ParquetReader<OneOfEach> reader = ThriftParquetReader.<OneOfEach>build(p).withThriftClass(OneOfEach.class).build();

    List<OneOfEach> found = new ArrayList<OneOfEach>();
    OneOfEach next = reader.read();
    while(next != null) {
      found.add(next);
      next = reader.read();
    }

    assertEquals(expected, found);
  }
}
