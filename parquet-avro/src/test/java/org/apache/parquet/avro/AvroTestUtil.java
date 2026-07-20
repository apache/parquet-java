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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.variant.Variant;

public class AvroTestUtil {

  public static Schema record(String name, String namespace, Schema.Field... fields) {
    Schema record = Schema.createRecord(name, null, namespace, false);
    record.setFields(Arrays.asList(fields));
    return record;
  }

  public static Schema record(String name, Schema.Field... fields) {
    return record(name, null, fields);
  }

  public static Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, null);
  }

  public static Schema.Field optionalField(String name, Schema schema) {
    return new Schema.Field(name, optional(schema), null, JsonProperties.NULL_VALUE);
  }

  public static Schema array(Schema element) {
    return Schema.createArray(element);
  }

  public static Schema primitive(Schema.Type type) {
    return Schema.create(type);
  }

  public static Schema optional(Schema original) {
    return Schema.createUnion(Lists.newArrayList(Schema.create(Schema.Type.NULL), original));
  }

  public static GenericRecord instance(Schema schema, Object... pairs) {
    if ((pairs.length % 2) != 0) {
      throw new RuntimeException("Not enough values");
    }
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < pairs.length; i += 2) {
      record.put(pairs[i].toString(), pairs[i + 1]);
    }
    return record;
  }

  public static <D> List<D> read(GenericData model, Schema schema, File file) throws IOException {
    return read(new Configuration(false), model, schema, file);
  }

  public static <D> List<D> read(Configuration conf, GenericData model, Schema schema, File file) throws IOException {
    List<D> data = new ArrayList<D>();
    AvroReadSupport.setRequestedProjection(conf, schema);
    AvroReadSupport.setAvroReadSchema(conf, schema);

    try (ParquetReader<D> fileReader = AvroParquetReader.<D>builder(
            HadoopInputFile.fromPath(new Path(file.toString()), conf))
        .withDataModel(model) // reflect disables compatibility
        .build()) {
      D datum;
      while ((datum = fileReader.read()) != null) {
        data.add(datum);
      }
    }

    return data;
  }

  @SuppressWarnings("unchecked")
  public static <D> File write(java.nio.file.Path tempDir, GenericData model, Schema schema, D... data)
      throws IOException {
    return write(tempDir, new Configuration(false), model, schema, data);
  }

  @SuppressWarnings("unchecked")
  public static <D> File write(
      java.nio.file.Path tempDir, Configuration conf, GenericData model, Schema schema, D... data)
      throws IOException {
    java.nio.file.Path file = tempDir.resolve("test");

    try (ParquetWriter<D> writer = AvroParquetWriter.<D>builder(new Path(file.toUri()))
        .withDataModel(model)
        .withSchema(schema)
        .build()) {
      for (D datum : data) {
        writer.write(datum);
      }
    }

    return file.toFile();
  }

  public static Configuration conf(String name, boolean value) {
    Configuration conf = new Configuration(false);
    conf.setBoolean(name, value);
    return conf;
  }

  /**
   * Assert that to Variant values are logically equivalent.
   * E.g. values in an object may be ordered differently in the binary.
   */
  static void assertEquivalent(Variant expected, Variant actual) {
    assertThat(actual.getType()).isEqualTo(expected.getType());
    switch (expected.getType()) {
      case STRING:
        // Short strings may use the compact or extended representation.
        assertThat(actual.getString()).isEqualTo(expected.getString());
        break;
      case ARRAY:
        assertThat(actual.numArrayElements()).isEqualTo(expected.numArrayElements());
        for (int i = 0; i < expected.numArrayElements(); ++i) {
          assertEquivalent(expected.getElementAtIndex(i), actual.getElementAtIndex(i));
        }
        break;
      case OBJECT:
        assertThat(actual.numObjectElements()).isEqualTo(expected.numObjectElements());
        for (int i = 0; i < expected.numObjectElements(); ++i) {
          Variant.ObjectField expectedField = expected.getFieldAtIndex(i);
          Variant.ObjectField actualField = actual.getFieldAtIndex(i);
          assertThat(actualField.key).isEqualTo(expectedField.key);
          assertEquivalent(expectedField.value, actualField.value);
        }
        break;
      default:
        // All other types have a single representation, and must be bit-for-bit identical.
        assertThat(actual.getValueBuffer()).isEqualTo(expected.getValueBuffer());
    }
  }
}
