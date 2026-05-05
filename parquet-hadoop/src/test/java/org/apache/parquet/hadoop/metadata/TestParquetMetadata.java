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
package org.apache.parquet.hadoop.metadata;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

public class TestParquetMetadata {
  private static final String EXPECTED_JSON = "/test-expected-parquet-metadata.json";

  // Use an object mapper, since order of the keys might differ in JSON maps,
  // and we don't want to sort them because of testing
  private static final ObjectMapper mapper = new ObjectMapper();

  private static JsonNode expectedJson() throws IOException, URISyntaxException {
    URI path = TestParquetMetadata.class.getResource(EXPECTED_JSON).toURI();
    return mapper.readTree(path.toURL());
  }

  @Test
  public void testToPrettyJSON() throws IOException, URISyntaxException {
    MessageType complexParquetSchema = Types.buildMessage()
        .addField(Types.optional(INT32)
            .as(LogicalTypeAnnotation.intType(8))
            .named("a"))
        .addField(Types.optionalGroup()
            .addField(Types.optional(INT32)
                .as(LogicalTypeAnnotation.intType(16))
                .named("c"))
            .addField(Types.optional(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("d"))
            .named("b"))
        .addField(Types.optionalList()
            .setElementType(Types.optional(INT32)
                .as(LogicalTypeAnnotation.dateType())
                .named("element"))
            .named("e"))
        .addField(Types.optionalList()
            .setElementType(Types.optional(INT32)
                .as(LogicalTypeAnnotation.dateType())
                .named("element"))
            .named("f"))
        .addField(Types.optional(FLOAT).named("g"))
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, MILLIS))
            .named("h"))
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, NANOS))
            .named("i"))
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(false, MILLIS))
            .named("j"))
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(true, MICROS))
            .named("k"))
        .addField(Types.optional(INT64)
            .as(LogicalTypeAnnotation.timestampType(false, MICROS))
            .named("l"))
        .addField(Types.optional(FIXED_LEN_BYTE_ARRAY)
            .length(12)
            .as(LogicalTypeAnnotation.intervalType())
            .named("m"))
        .addField(Types.optionalMap()
            .key(Types.optional(INT32)
                .as(LogicalTypeAnnotation.dateType())
                .named("key"))
            .value(Types.optional(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("value"))
            .named("list"))
        .named("root");

    FileMetaData fmd = new FileMetaData(complexParquetSchema, Collections.emptyMap(), "ASF");
    String prettyJSon = ParquetMetadata.toPrettyJSON(new ParquetMetadata(fmd, Collections.emptyList()));

    assertEquals(mapper.readTree(prettyJSon), expectedJson());
  }
}
