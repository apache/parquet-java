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
package org.apache.parquet.column.values.alp;

import static org.junit.Assert.*;

import org.junit.Test;

public class AlpConstantsTest {

  @Test
  public void testHeaderSize() {
    assertEquals(7, AlpConstants.HEADER_SIZE);
  }

  @Test
  public void testFloatPow10Table() {
    assertEquals(11, AlpConstants.FLOAT_POW10.length);
    assertEquals(1.0f, AlpConstants.FLOAT_POW10[0], 0.0f);
    assertEquals(10.0f, AlpConstants.FLOAT_POW10[1], 0.0f);
    assertEquals(1e10f, AlpConstants.FLOAT_POW10[10], 0.0f);
  }

  @Test
  public void testDoublePow10Table() {
    assertEquals(19, AlpConstants.DOUBLE_POW10.length);
    assertEquals(1.0, AlpConstants.DOUBLE_POW10[0], 0.0);
    assertEquals(10.0, AlpConstants.DOUBLE_POW10[1], 0.0);
    assertEquals(1e18, AlpConstants.DOUBLE_POW10[18], 0.0);
  }

  @Test
  public void testIntegerPow10() {
    assertEquals(1L, AlpConstants.integerPow10(0));
    assertEquals(10L, AlpConstants.integerPow10(1));
    assertEquals(100L, AlpConstants.integerPow10(2));
    assertEquals(1_000_000_000L, AlpConstants.integerPow10(9));
    assertEquals(1_000_000_000_000_000_000L, AlpConstants.integerPow10(18));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIntegerPow10NegativePower() {
    AlpConstants.integerPow10(-1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIntegerPow10TooLargePower() {
    AlpConstants.integerPow10(19);
  }

  @Test
  public void testValidateVectorSize() {
    assertEquals(8, AlpConstants.validateVectorSize(8));
    assertEquals(1024, AlpConstants.validateVectorSize(1024));
    assertEquals(32768, AlpConstants.validateVectorSize(32768));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateVectorSizeNotPowerOf2() {
    AlpConstants.validateVectorSize(100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateVectorSizeTooSmall() {
    AlpConstants.validateVectorSize(4); // 2^2 < MIN_LOG=3
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateVectorSizeTooLarge() {
    AlpConstants.validateVectorSize(65536); // 2^16 > MAX_LOG=15
  }

  @Test
  public void testEncodingLimits() {
    assertTrue(AlpConstants.FLOAT_ENCODING_UPPER_LIMIT > 0);
    assertTrue(AlpConstants.FLOAT_ENCODING_LOWER_LIMIT < 0);
    assertTrue(AlpConstants.DOUBLE_ENCODING_UPPER_LIMIT > 0);
    assertTrue(AlpConstants.DOUBLE_ENCODING_LOWER_LIMIT < 0);
  }

  @Test
  public void testMetadataSizes() {
    assertEquals(4, AlpConstants.ALP_INFO_SIZE);
    assertEquals(5, AlpConstants.FLOAT_FOR_INFO_SIZE);
    assertEquals(9, AlpConstants.DOUBLE_FOR_INFO_SIZE);
  }
}
