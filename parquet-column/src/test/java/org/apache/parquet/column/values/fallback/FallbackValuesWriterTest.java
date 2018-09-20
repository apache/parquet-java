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
package org.apache.parquet.column.values.fallback;

import static org.junit.Assert.assertEquals;

import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.junit.Test;
import org.mockito.Mockito;

public class FallbackValuesWriterTest {

  @Test
  public void testGetBufferedSize() {
    DictionaryValuesWriter dictionaryValuesWriterMock = Mockito.mock(DictionaryValuesWriter.class);
    ValuesWriter plainValuesWriterMock = Mockito.mock(ValuesWriter.class);
    FallbackValuesWriter<DictionaryValuesWriter, ValuesWriter> fallbackValuesWriter =
        FallbackValuesWriter.of(dictionaryValuesWriterMock, plainValuesWriterMock);
    fallbackValuesWriter.rawDataByteSize = 100_050L;
    Mockito.when(dictionaryValuesWriterMock.getBufferedSize()).thenReturn(50L);
    Mockito.when(dictionaryValuesWriterMock.getUtilization()).thenReturn(0.0, 0.2, 0.4, 0.6, 0.7, 0.8, 0.9, 1.0);
    // Use an epsilon of 1 for comparisons, since the calculations use double
    // arithmetics and are not 100% accurate, but that's acceptable.

    // Utilization below the threshold of 0.6, buffered size should return the dictionary-encoded size.
    assertEquals(50L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.0
    assertEquals(50L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.2
    assertEquals(50L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.4
    assertEquals(50L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.6

    // Utilization goes above the threshold of 0.6, buffered size should gradually increase up to the raw data size. 
    assertEquals(25_050L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.7
    assertEquals(50_050L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.8
    assertEquals(75_050L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 0.9
    
    // Utilization reaches 100%, buffered size should return the raw data size. 
    assertEquals(100_050L, fallbackValuesWriter.getBufferedSize(), 1); // Utilization = 1.0
  }
}
