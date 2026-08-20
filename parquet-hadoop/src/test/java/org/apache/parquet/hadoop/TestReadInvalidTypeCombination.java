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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.ColumnOrder.ColumnOrderName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;

public class TestReadInvalidTypeCombination {

  // Path to a Parquet file that contains an invalid logical/physical type combination.
  private static final String FILE_PATH = "/invalid_type_combination.parquet";

  private static Path getFilePath() throws Exception {
    return new Path(TestReadInvalidTypeCombination.class.getResource(FILE_PATH).toURI());
  }

  @Test
  public void testReadInvalidTypeCombinationSucceeds() throws Exception {
    Configuration conf = new Configuration();
    Path file = getFilePath();

    // The footer parse should succeed and drop the annotation and stats for the column.
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(file, conf))) {
      ParquetMetadata footer = reader.getFooter();
      PrimitiveType column =
          footer.getFileMetaData().getSchema().getType("int32_uuid").asPrimitiveType();

      assertThat(column.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.INT32);
      assertThat(column.getLogicalTypeAnnotation()).isNull();
      assertThat(column.columnOrder().getColumnOrderName()).isEqualTo(ColumnOrderName.UNDEFINED);
    }

    // The physical values are still fully readable.
    int rows = 0;
    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file)
        .withConf(conf)
        .build()) {
      Group g;
      while ((g = reader.read()) != null) {
        assertThat(g.getInteger("int32_uuid", 0)).isEqualTo(rows);
        rows++;
      }
    }
    assertThat(rows).isEqualTo(10);
  }
}
