/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.avro.Schema;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

public class TestAvroIndexedRecordConverter {

  @Test
  public void testExactFieldNameTakesPrecedenceOverAlias() {
    Schema avroSchema = new Schema.Parser()
        .parse("{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"record\",\n"
            + "  \"fields\": [\n"
            + "    {\"name\": \"renamed\", \"type\": [\"null\", \"string\"], \"default\": null, \"aliases\": [\"value\"]},\n"
            + "    {\"name\": \"value\", \"type\": \"string\"}\n"
            + "  ]\n"
            + "}");
    MessageType parquetSchema =
        MessageTypeParser.parseMessageType("message record { required binary value (STRING); }");

    AvroIndexedRecordConverter<?> converter = new AvroIndexedRecordConverter<>(parquetSchema, avroSchema);
    converter.start();
    ((PrimitiveConverter) converter.getConverter(0)).addBinary(Binary.fromString("expected"));
    converter.end();

    assertThat(converter.getCurrentRecord().get(1)).isEqualTo("expected");
    assertThat(converter.getCurrentRecord().get(0)).isNull();
  }

  @Test
  public void testAliasIsUsedWhenExactFieldNameIsMissing() {
    Schema avroSchema = new Schema.Parser()
        .parse("{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"record\",\n"
            + "  \"fields\": [\n"
            + "    {\"name\": \"renamed\", \"type\": \"string\", \"aliases\": [\"value\"]}\n"
            + "  ]\n"
            + "}");
    MessageType parquetSchema =
        MessageTypeParser.parseMessageType("message record { required binary value (STRING); }");

    AvroIndexedRecordConverter<?> converter = new AvroIndexedRecordConverter<>(parquetSchema, avroSchema);
    converter.start();
    ((PrimitiveConverter) converter.getConverter(0)).addBinary(Binary.fromString("expected"));
    converter.end();

    assertThat(converter.getCurrentRecord().get(0)).isEqualTo("expected");
  }
}
