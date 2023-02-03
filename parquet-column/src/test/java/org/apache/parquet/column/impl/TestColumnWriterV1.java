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
package org.apache.parquet.column.impl;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestColumnWriterV1 {

  @Test
  public void testEstimateNextSizeCheckOnZeroUsedMem() throws Exception {
    ColumnDescriptor col = new ColumnDescriptor(
        new String[] { "val" },
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.INT32, "val"),
        0, 0);
    PageWriter pageWriter = mock(PageWriter.class);
    ParquetProperties props = ParquetProperties.builder().build();
    ColumnWriterV1 columnWriter = new ColumnWriterV1(col, pageWriter, props);

    // Write enough NULLs to exceed initial valueCountForNextSizeCheck which is
    // initialized to MinRowCountForPageSizeCheck. These NULLs result in used
    // memSize == 0. Estimating valueCountForNextSizeCheck needs to handle that
    // correctly.
    for (int i = 0; i < props.getMinRowCountForPageSizeCheck() * 2; i++) {
      columnWriter.writeNull(0, 0);
    }

    // Emit enough data to trigger used memSize exceed page size. This should
    // trigger writePage() if valueCountForNextSizeCheck is estimated correctly.
    for (int i = 0; i < props.getPageSizeThreshold() * 2; i++) {
      columnWriter.write(i, 0, 0);
    }

    // Verify writePage() must have been called
    verify(pageWriter, atLeastOnce())
        .writePage(any(), anyInt(), any(), any(), any(), any());
  }

}
