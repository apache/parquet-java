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

import com.google.common.collect.Lists;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.Assert.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AvroRecordConverter.class)
public class TestAvroRecordConverter {

  @Before
  public void setup() {
    // Default to calling real methods unless overridden in specific test
    PowerMockito.mockStatic(AvroRecordConverter.class, CALLS_REAL_METHODS);
  }

  @Test
  public void testModelForSpecificRecordWithLogicalTypes() {
    SpecificData model = AvroRecordConverter.getModelForSchema(LogicalTypesTest.SCHEMA$);

    // Test that model is generated correctly
    Conversion<?> conversion = model.getConversionByClass(Instant.class);
    assertEquals(TimeConversions.TimestampMillisConversion.class, conversion.getClass());
  }

  @Test
  public void testModelForSpecificRecordWithoutLogicalTypes() {
    SpecificData model = AvroRecordConverter.getModelForSchema(Car.SCHEMA$);

    assertTrue(model.getConversions().isEmpty());
  }

  @Test
  public void testModelForGenericRecord() {
    SpecificData model = AvroRecordConverter.getModelForSchema(
      Schema.createRecord(
        "someSchema",
        "doc",
        "some.namespace",
        false,
        Lists.newArrayList(new Schema.Field("strField", Schema.create(Schema.Type.STRING)))));

    // There is no class "someSchema" on the classpath, so should return null
    assertNull(model);
  }

  // Test logical type support for older Avro versions
  @Test
  public void testGetModelAvro1_7() {
    BDDMockito.given(AvroRecordConverter.getRuntimeAvroVersion()).willReturn("1.7.7");

    // Test that model is generated correctly
    final SpecificData model = AvroRecordConverter.getModelForSchema(Avro17GeneratedClass.SCHEMA$);
    Conversion<?> conversion = model.getConversionByClass(BigDecimal.class);
    assertEquals(Conversions.DecimalConversion.class, conversion.getClass());
  }

  @Test
  public void testGetModelAvro1_8() {
    BDDMockito.given(AvroRecordConverter.getRuntimeAvroVersion()).willReturn("1.8.2");

    // Test that model is generated correctly
    final SpecificData model = AvroRecordConverter.getModelForSchema(Avro18GeneratedClass.SCHEMA$);
    Conversion<?> conversion = model.getConversionByClass(BigDecimal.class);
    assertEquals(Conversions.DecimalConversion.class, conversion.getClass());
  }

  @Test
  public void testGetModelAvro1_9() {
    BDDMockito.given(AvroRecordConverter.getRuntimeAvroVersion()).willReturn("1.9.2");

    // Test that model is generated correctly
    final SpecificData model = AvroRecordConverter.getModelForSchema(Avro19GeneratedClass.SCHEMA$);
    Conversion<?> conversion = model.getConversionByClass(BigDecimal.class);
    assertEquals(Conversions.DecimalConversion.class, conversion.getClass());
  }

  @Test
  public void testGetModelAvro1_10() {
    BDDMockito.given(AvroRecordConverter.getRuntimeAvroVersion()).willReturn("1.10.2");

    // Test that model is generated correctly
    final SpecificData model = AvroRecordConverter.getModelForSchema(Avro110GeneratedClass.SCHEMA$);
    Conversion<?> conversion = model.getConversionByClass(BigDecimal.class);
    assertEquals(Conversions.DecimalConversion.class, conversion.getClass());
  }

  // Test Avro record class stubs, generated using different versions of the Avro compiler
  public abstract static class Avro110GeneratedClass extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 5558880508010468207L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Avro110GeneratedClass\",\"namespace\":\"org.apache.parquet.avro.TestAvroRecordConverter\",\"doc\":\"\",\"fields\":[{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}");

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();

    static {
      MODEL$.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
    }
  }

  public abstract static class Avro19GeneratedClass extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 5558880508010468207L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Avro19GeneratedClass\",\"namespace\":\"org.apache.parquet.avro.TestAvroRecordConverter\",\"doc\":\"\",\"fields\":[{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}");

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();

    static {
      MODEL$.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
    }
  }

  public abstract static class Avro18GeneratedClass extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 5558880508010468207L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Avro18GeneratedClass\",\"namespace\":\"org.apache.parquet.avro.TestAvroRecordConverter\",\"doc\":\"\",\"fields\":[{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}");

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();

    protected static final org.apache.avro.Conversions.DecimalConversion DECIMAL_CONVERSION = new org.apache.avro.Conversions.DecimalConversion();

    private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
        DECIMAL_CONVERSION,
        null
      };

    @Override
    public org.apache.avro.Conversion<?> getConversion(int field) {
      return conversions[field];
    }
  }

  public abstract static class Avro17GeneratedClass extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 5558880508010468207L;
    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Avro17GeneratedClass\",\"namespace\":\"org.apache.parquet.avro.TestAvroRecordConverter\",\"doc\":\"\",\"fields\":[{\"name\":\"decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":2}}]}");

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA$;
    }

    private static SpecificData MODEL$ = new SpecificData();

    protected static final org.apache.avro.Conversions.DecimalConversion DECIMAL_CONVERSION = new org.apache.avro.Conversions.DecimalConversion();

    private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
        DECIMAL_CONVERSION,
        null
      };

    @Override
    public org.apache.avro.Conversion<?> getConversion(int field) {
      return conversions[field];
    }
  }
}
