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
package org.apache.parquet.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestAvroFieldNameAndAlias {

  @TempDir
  private java.nio.file.Path tempDir;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void exactFieldNameTakesPrecedenceOverAlias(boolean compat) throws IOException {
    Schema writerSchema = AvroTestUtil.record("Car", AvroTestUtil.field("make", Schema.create(Schema.Type.STRING)));
    GenericRecord written = AvroTestUtil.instance(writerSchema, "make", "Volkswagen");
    File file = AvroTestUtil.write(tempDir, GenericData.get(), writerSchema, written);

    Configuration conf = new Configuration(false);
    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, compat);

    // File has only "make". The reader still has a "make" field, plus an earlier
    // "brand" field that aliases "make". An alias-first lookup writes the parquet
    // value into brand and leaves make as its default.
    Schema.Field brand = new Schema.Field("brand", Schema.create(Schema.Type.STRING), null, "unmapped-brand");
    brand.addAlias("make");
    Schema.Field make = new Schema.Field("make", Schema.create(Schema.Type.STRING), null, "unmapped-make");
    Schema readerSchema = AvroTestUtil.record("Car", brand, make);
    assertThat(readerSchema.getField("make")).isNotNull();
    assertThat(readerSchema.getField("brand").aliases()).contains("make");

    AvroReadSupport.setAvroReadSchema(conf, readerSchema);

    List<GenericRecord> records = new ArrayList<>();
    try (ParquetReader<GenericRecord> reader = new AvroParquetReader<>(conf, new Path(file.toString()))) {
      records.add(reader.read());
    }

    assertThat(records).hasSize(1);
    GenericRecord read = records.get(0);
    assertThat(read.get("make")).asString().isEqualTo("Volkswagen");
    assertThat(read.get("brand")).asString().isEqualTo("unmapped-brand");
  }
}
