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

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class TestInterOpReadUnknownLogicalType {
  private static final String REFERENCE_FILE = "unknown-logical-type.parquet";
  private static final String REFERENCE_CHANGESET = "1a2a75127be06fc0123f03ebd36c966f7beda27d";
  private static final String KNOWN_COLUMN = "column with known type";
  private static final String UNKNOWN_COLUMN = "column with unknown type";

  private final InterOpTester interop = new InterOpTester();

  @Test
  public void testUnknownLogicalTypePreservesPhysicalType() throws IOException {
    Configuration conf = new Configuration();
    Path file = interop.GetInterOpFile(REFERENCE_FILE, REFERENCE_CHANGESET);

    try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf));
        ParquetReader<Group> recordReader = ParquetReader.builder(new GroupReadSupport(), file)
            .withConf(conf)
            .build()) {
      Type knownColumn =
          fileReader.getFooter().getFileMetaData().getSchema().getType(KNOWN_COLUMN);
      assertEquals(PrimitiveTypeName.BINARY, knownColumn.asPrimitiveType().getPrimitiveTypeName());
      assertEquals(stringType(), knownColumn.getLogicalTypeAnnotation());

      Type unknownColumn =
          fileReader.getFooter().getFileMetaData().getSchema().getType(UNKNOWN_COLUMN);
      assertEquals(
          PrimitiveTypeName.BINARY, unknownColumn.asPrimitiveType().getPrimitiveTypeName());
      assertNull(unknownColumn.getLogicalTypeAnnotation());

      int rows = 0;
      Group group;
      while ((group = recordReader.read()) != null) {
        rows += 1;
        assertEquals(
            "known string " + rows, group.getBinary(KNOWN_COLUMN, 0).toStringUsingUTF8());
        assertEquals(
            "unknown string " + rows,
            group.getBinary(UNKNOWN_COLUMN, 0).toStringUsingUTF8());
      }
      assertEquals(3, rows);
    }
  }
}
