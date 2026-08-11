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
package org.apache.parquet.bytes;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class TestBytesUtil {

  @Test
  public void testWidth() {
    assertThat(getWidthFromMaxInt(0)).isEqualTo(0);
    assertThat(getWidthFromMaxInt(1)).isEqualTo(1);
    assertThat(getWidthFromMaxInt(2)).isEqualTo(2);
    assertThat(getWidthFromMaxInt(3)).isEqualTo(2);
    assertThat(getWidthFromMaxInt(4)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(5)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(6)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(7)).isEqualTo(3);
    assertThat(getWidthFromMaxInt(8)).isEqualTo(4);
    assertThat(getWidthFromMaxInt(15)).isEqualTo(4);
    assertThat(getWidthFromMaxInt(16)).isEqualTo(5);
    assertThat(getWidthFromMaxInt(31)).isEqualTo(5);
    assertThat(getWidthFromMaxInt(32)).isEqualTo(6);
    assertThat(getWidthFromMaxInt(63)).isEqualTo(6);
    assertThat(getWidthFromMaxInt(64)).isEqualTo(7);
    assertThat(getWidthFromMaxInt(127)).isEqualTo(7);
    assertThat(getWidthFromMaxInt(128)).isEqualTo(8);
    assertThat(getWidthFromMaxInt(255)).isEqualTo(8);
  }
}
