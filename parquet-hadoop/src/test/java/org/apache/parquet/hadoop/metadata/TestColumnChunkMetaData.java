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
package org.apache.parquet.hadoop.metadata;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;

public class TestColumnChunkMetaData {

  @Test
  public void testConversionBig() {
    long big = (long) Integer.MAX_VALUE + 1;

    ColumnChunkMetaData md = newMD(big);
    assertTrue(md instanceof IntColumnChunkMetaData);
    assertEquals(big, md.getFirstDataPageOffset());
  }

  @Test
  public void testConversionSmall() {
    long small = 1;

    ColumnChunkMetaData md = newMD(small);
    assertTrue(md instanceof IntColumnChunkMetaData);
    assertEquals(small, md.getFirstDataPageOffset());
  }

  @Test
  public void testConversionVeryBig() {
    long veryBig = (long) Integer.MAX_VALUE * 3;

    ColumnChunkMetaData md = newMD(veryBig);
    assertTrue(md instanceof LongColumnChunkMetaData);
    assertEquals(veryBig, md.getFirstDataPageOffset());
  }

  @Test
  public void testConversionNeg() {
    long neg = -1;

    ColumnChunkMetaData md = newMD(neg);
    assertTrue(md instanceof LongColumnChunkMetaData);
    assertEquals(neg, md.getFirstDataPageOffset());
  }

  private ColumnChunkMetaData newMD(long big) {
    Set<Encoding> e = new HashSet<Encoding>();
    PrimitiveTypeName t = BINARY;
    ColumnPath p = ColumnPath.get("foo");
    CompressionCodecName c = CompressionCodecName.GZIP;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, e, s, big, 0, 0, 0, 0);
    return md;
  }
}
