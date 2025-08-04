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

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.Preconditions;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantConverters;

/**
 * Converter for Variant values.
 */
class AvroVariantConverter extends GroupConverter implements VariantConverters.ParentConverter<VariantBuilder> {
  private static final Schema VARIANT_SCHEMA = SchemaBuilder.record("VariantRecord")
      .fields()
      .name("metadata")
      .type()
      .bytesType()
      .noDefault()
      .name("value")
      .type()
      .bytesType()
      .noDefault()
      .endRecord();

  private final ParentValueContainer parent;
  private final GenericData model;
  private final GroupConverter wrappedConverter;

  private VariantBuilder builder = null;
  private ImmutableMetadata metadata = null;

  AvroVariantConverter(ParentValueContainer parent, GroupType variantGroup, Schema avroSchema, GenericData model) {
    this.parent = parent;
    this.model = model;
    this.wrappedConverter = VariantConverters.newVariantConverter(variantGroup, this::setMetadata, this);
  }

  @Override
  public void build(Consumer<VariantBuilder> consumer) {
    Preconditions.checkState(builder != null, "Cannot build variant: builder has not been initialized");
    consumer.accept(builder);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return wrappedConverter.getConverter(fieldIndex);
  }

  @Override
  public void start() {
    wrappedConverter.start();
  }

  @Override
  public void end() {
    wrappedConverter.end();

    Preconditions.checkState(metadata != null, "Cannot build variant: missing metadata");

    builder.appendNullIfEmpty();

    Object record = model.newRecord(null, VARIANT_SCHEMA);
    model.setField(record, "metadata", 0, metadata.getEncodedBuffer());
    model.setField(record, "value", 1, builder.encodedValue());
    parent.add(record);

    this.builder = null;
  }

  void setMetadata(ByteBuffer metadataBuffer) {
    // If the metadata hasn't changed, we don't need to rebuild the map.
    if (metadata == null || metadata.getEncodedBuffer() != metadataBuffer) {
      this.metadata = new ImmutableMetadata(metadataBuffer);
    }

    this.builder = new VariantBuilder(metadata);
  }
}
