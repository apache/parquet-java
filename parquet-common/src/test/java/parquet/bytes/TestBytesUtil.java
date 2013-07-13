/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.bytes;

import static org.junit.Assert.assertEquals;
import static parquet.bytes.BytesUtils.getWidthFromMaxInt;

import org.junit.Test;

public class TestBytesUtil {

  @Test
  public void testWidth() {
    assertEquals(0, getWidthFromMaxInt(0));
    assertEquals(1, getWidthFromMaxInt(1));
    assertEquals(2, getWidthFromMaxInt(2));
    assertEquals(2, getWidthFromMaxInt(3));
    assertEquals(3, getWidthFromMaxInt(4));
    assertEquals(3, getWidthFromMaxInt(5));
    assertEquals(3, getWidthFromMaxInt(6));
    assertEquals(3, getWidthFromMaxInt(7));
    assertEquals(4, getWidthFromMaxInt(8));
    assertEquals(4, getWidthFromMaxInt(15));
    assertEquals(5, getWidthFromMaxInt(16));
    assertEquals(5, getWidthFromMaxInt(31));
    assertEquals(6, getWidthFromMaxInt(32));
    assertEquals(6, getWidthFromMaxInt(63));
    assertEquals(7, getWidthFromMaxInt(64));
    assertEquals(7, getWidthFromMaxInt(127));
    assertEquals(8, getWidthFromMaxInt(128));
    assertEquals(8, getWidthFromMaxInt(255));
  }
}
