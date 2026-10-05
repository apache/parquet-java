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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class AlpConstantsTest {

  @Test
  public void testHeaderSize() {
    assertThat(AlpConstants.HEADER_SIZE).isEqualTo(7);
  }

  @Test
  public void testFloatPow10Table() {
    assertThat(AlpConstants.FLOAT_POW10).hasSize(11);
    assertThat(AlpConstants.FLOAT_POW10[0]).isEqualTo(1.0f);
    assertThat(AlpConstants.FLOAT_POW10[1]).isEqualTo(10.0f);
    assertThat(AlpConstants.FLOAT_POW10[10]).isEqualTo(1e10f);
  }

  @Test
  public void testDoublePow10Table() {
    assertThat(AlpConstants.DOUBLE_POW10).hasSize(19);
    assertThat(AlpConstants.DOUBLE_POW10[0]).isEqualTo(1.0);
    assertThat(AlpConstants.DOUBLE_POW10[1]).isEqualTo(10.0);
    assertThat(AlpConstants.DOUBLE_POW10[18]).isEqualTo(1e18);
  }

  @Test
  public void testIntegerPow10() {
    assertThat(AlpConstants.integerPow10(0)).isEqualTo(1L);
    assertThat(AlpConstants.integerPow10(1)).isEqualTo(10L);
    assertThat(AlpConstants.integerPow10(2)).isEqualTo(100L);
    assertThat(AlpConstants.integerPow10(9)).isEqualTo(1_000_000_000L);
    assertThat(AlpConstants.integerPow10(18)).isEqualTo(1_000_000_000_000_000_000L);
  }

  @Test
  public void testIntegerPow10NegativePower() {
    assertThatThrownBy(() -> AlpConstants.integerPow10(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("power must be in [0, 18], got: -1");
  }

  @Test
  public void testIntegerPow10TooLargePower() {
    assertThatThrownBy(() -> AlpConstants.integerPow10(19))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("power must be in [0, 18], got: 19");
  }

  @Test
  public void testValidateVectorSize() {
    assertThat(AlpConstants.validateVectorSize(8)).isEqualTo(8);
    assertThat(AlpConstants.validateVectorSize(1024)).isEqualTo(1024);
    assertThat(AlpConstants.validateVectorSize(32768)).isEqualTo(32768);
  }

  @Test
  public void testValidateVectorSizeNotPowerOf2() {
    assertThatThrownBy(() -> AlpConstants.validateVectorSize(100))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Vector size must be a power of 2, got: 100");
  }

  @Test
  public void testValidateVectorSizeTooSmall() {
    assertThatThrownBy(() -> AlpConstants.validateVectorSize(4))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Vector size log2 must be between 3 and 15, got: 2 (vectorSize=4)");
  }

  @Test
  public void testValidateVectorSizeTooLarge() {
    assertThatThrownBy(() -> AlpConstants.validateVectorSize(65536))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Vector size log2 must be between 3 and 15, got: 16 (vectorSize=65536)");
  }

  @Test
  public void testEncodingLimits() {
    assertThat(AlpConstants.FLOAT_ENCODING_UPPER_LIMIT).isPositive();
    assertThat(AlpConstants.FLOAT_ENCODING_LOWER_LIMIT).isNegative();
    assertThat(AlpConstants.DOUBLE_ENCODING_UPPER_LIMIT).isPositive();
    assertThat(AlpConstants.DOUBLE_ENCODING_LOWER_LIMIT).isNegative();
  }

  @Test
  public void testMetadataSizes() {
    assertThat(AlpConstants.ALP_INFO_SIZE).isEqualTo(4);
    assertThat(AlpConstants.FLOAT_FOR_INFO_SIZE).isEqualTo(5);
    assertThat(AlpConstants.DOUBLE_FOR_INFO_SIZE).isEqualTo(9);
  }
}
