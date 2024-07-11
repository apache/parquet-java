/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.avro;

import static org.apache.parquet.hadoop.TestParquetReader.FILE_V1;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.junit.Test;

public class TestAvroParquetReader {

  @Test
  public void testConstructor() throws IOException {
    InputFile inputFile = new LocalInputFile(Paths.get(FILE_V1.toUri().getRawPath()));
    ParquetReader<Group> reader =
        AvroParquetReader.<Group>builder(inputFile).build();
    assertNotNull(reader);

    reader = AvroParquetReader.<Group>builder(inputFile, new HadoopParquetConfiguration(new Configuration()))
        .build();
    assertNotNull(reader);

    reader = AvroParquetReader.builder(
            new GroupReadSupport(), new Path(FILE_V1.toUri().getRawPath()))
        .build();
    assertNotNull(reader);
  }
}
