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
import static org.mockito.Mockito.CALLS_REAL_METHODS;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Collection;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class TestAvroRecordConverter {

  private MockedStatic<AvroRecordConverter> avroRecordConverterMock;

  @BeforeEach
  public void setup() {
    // Default to calling real methods unless overridden in specific test
    avroRecordConverterMock = Mockito.mockStatic(AvroRecordConverter.class, CALLS_REAL_METHODS);
  }

  @AfterEach
  public void tearDown() {
    avroRecordConverterMock.close();
  }

  @Test
  public void testModelForSpecificRecordWithLogicalTypes() {
    SpecificData model = AvroRecordConverter.getModelForSchema(LogicalTypesTest.SCHEMA$);

    // Test that model is generated correctly
    Collection<Conversion<?>> conversions = model.getConversions();
    assertThat(conversions).hasSize(3);
    assertThat(model.getConversionByClass(Instant.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalDate.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalTime.class)).isNotNull();
  }

  @Test
  public void testModelForSpecificRecordWithoutLogicalTypes() {
    SpecificData model = AvroRecordConverter.getModelForSchema(Car.SCHEMA$);
    assertThat(model.getConversions()).isEmpty();
  }

  @Test
  public void testModelForGenericRecord() {
    SpecificData model = AvroRecordConverter.getModelForSchema(Schema.createRecord(
        "someSchema",
        "doc",
        "some.namespace",
        false,
        Lists.newArrayList(new Schema.Field("strField", Schema.create(Schema.Type.STRING)))));

    // There is no class "someSchema" on the classpath, so should return null
    assertThat(model).isNull();
  }

  // Test logical type support for older Avro versions
  @Test
  public void testModelForSpecificRecordWithLogicalTypesWithDeprecatedAvro1_8() {
    avroRecordConverterMock.when(AvroRecordConverter::getRuntimeAvroVersion).thenReturn("1.8.2");

    // Test that model is generated correctly when record contains both top-level and nested logical types
    SpecificData model = AvroRecordConverter.getModelForSchema(LogicalTypesTestDeprecated.SCHEMA$);
    // Test that model is generated correctly
    Collection<Conversion<?>> conversions = model.getConversions();
    assertThat(conversions).hasSize(3);
    assertThat(model.getConversionByClass(Instant.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalDate.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalTime.class)).isNotNull();

    // Test that model is generated correctly when record contains only nested logical types
    model = AvroRecordConverter.getModelForSchema(NestedOnlyLogicalTypesDeprecated.SCHEMA$);
    // Test that model is generated correctly
    conversions = model.getConversions();
    assertThat(conversions).hasSize(2);
    assertThat(model.getConversionByClass(LocalDate.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalTime.class)).isNotNull();
  }

  @Test
  public void testModelForSpecificRecordWithLogicalTypesWithDeprecatedAvro1_7() {
    avroRecordConverterMock.when(AvroRecordConverter::getRuntimeAvroVersion).thenReturn("1.7.7");

    // Test that model is generated correctly
    final SpecificData model = AvroRecordConverter.getModelForSchema(LogicalTypesTestDeprecated.SCHEMA$);
    // Test that model is generated correctly
    Collection<Conversion<?>> conversions = model.getConversions();
    assertThat(conversions).hasSize(3);
    assertThat(model.getConversionByClass(Instant.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalDate.class)).isNotNull();
    assertThat(model.getConversionByClass(LocalTime.class)).isNotNull();
  }

  // Pseudo generated code with bug from avro compiler < 1.8
  @org.apache.avro.specific.AvroGenerated
  public abstract static class LocalDateTimeTestDeprecated extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = SchemaBuilder.builder()
        .record("LocalDateTimeTestDeprecated")
        .namespace("org.apache.parquet.avro.TestAvroRecordConverter")
        .fields()
        .name("date")
        .type(LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType()))
        .noDefault()
        .name("time")
        .type(LogicalTypes.timeMillis()
            .addToSchema(SchemaBuilder.builder().intType()))
        .noDefault()
        .endRecord();

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();
    // this part is missing in the generated code
    // static {
    //   MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
    //   MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimeMillisConversion());
    // }

    private static final org.apache.avro.Conversion<?>[] conversions = new org.apache.avro.Conversion<?>[] {
      new org.apache.avro.data.TimeConversions.DateConversion(),
      new org.apache.avro.data.TimeConversions.TimeMillisConversion(),
      null
    };
  }

  // An Avro class generated from Avro 1.8 that contains both nested and top-level logical type fields
  @org.apache.avro.specific.AvroGenerated
  public abstract static class LogicalTypesTestDeprecated extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = SchemaBuilder.builder()
        .record("LogicalTypesTestDeprecated")
        .namespace("org.apache.parquet.avro.TestAvroRecordConverter")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.timestampMillis()
            .addToSchema(SchemaBuilder.builder().longType()))
        .noDefault()
        .name("local_date_time")
        .type(LocalDateTimeTestDeprecated.getClassSchema())
        .noDefault()
        .endRecord();

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();
    // this part is missing in the generated code
    // {
    // MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
    // MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
    // MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimeMillisConversion());
    // }

    private static final org.apache.avro.Conversion<?>[] conversions = new org.apache.avro.Conversion<?>[] {
      new org.apache.avro.data.TimeConversions.TimestampMillisConversion(), null, null
    };
  }

  // An Avro class generated from Avro 1.8 that contains only nested logical type fields
  @org.apache.avro.specific.AvroGenerated
  public abstract static class NestedOnlyLogicalTypesDeprecated extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    public static final org.apache.avro.Schema SCHEMA$ = SchemaBuilder.builder()
        .record("NestedOnlyLogicalTypesDeprecated")
        .namespace("org.apache.parquet.avro.TestAvroRecordConverter")
        .fields()
        .name("local_date_time")
        .type(LocalDateTimeTestDeprecated.getClassSchema())
        .noDefault()
        .endRecord();

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();

    // No top-level conversions field, since logical types are all nested
  }
}
