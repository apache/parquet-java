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
package org.apache.parquet.schema.converters;

import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.NANOS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.mapType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.example.Paper;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetSchemaConverter {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSchemaConverter() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(Paper.schema);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(Paper.schema, schema);
  }

  @Test
  public void testSchemaConverterDecimal() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    List<SchemaElement> schemaElements = parquetMetadataConverter.toParquetSchema(Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinaryDecimal")
        .optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aFixedDecimal")
        .named("Message"));
    List<SchemaElement> expected = List.of(
        new SchemaElement("Message").setNum_children(2),
        new SchemaElement("aBinaryDecimal")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setType(org.apache.parquet.format.Type.BYTE_ARRAY)
            .setConverted_type(ConvertedType.DECIMAL)
            .setLogicalType(LogicalType.DECIMAL(new DecimalType(2, 9)))
            .setPrecision(9)
            .setScale(2),
        new SchemaElement("aFixedDecimal")
            .setRepetition_type(FieldRepetitionType.OPTIONAL)
            .setType(Type.FIXED_LEN_BYTE_ARRAY)
            .setType_length(4)
            .setConverted_type(ConvertedType.DECIMAL)
            .setLogicalType(LogicalType.DECIMAL(new DecimalType(2, 9)))
            .setPrecision(9)
            .setScale(2));
    Assert.assertEquals(expected, schemaElements);
  }

  @Test
  public void testLogicalTypesBackwardCompatibleWithConvertedTypes() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    MessageType expected = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinaryDecimal")
        .named("Message");
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    // Set logical type field to null to test backward compatibility with files written by older API,
    // where converted_types are written to the metadata, but logicalType is missing
    parquetSchema.get(1).setLogicalType(null);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
  }

  @Test
  public void testIncompatibleLogicalAndConvertedTypes() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    MessageType schema = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinary")
        .named("Message");
    MessageType expected = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.jsonType())
        .named("aBinary")
        .named("Message");

    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    // Set converted type field to a different type to verify that in case of mismatch, it overrides logical type
    parquetSchema.get(1).setConverted_type(ConvertedType.JSON);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, actual);
  }

  @Test
  public void testTimeLogicalTypes() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    MessageType expected = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timestampType(false, MILLIS))
        .named("aTimestampNonUtcMillis")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timestampType(true, MILLIS))
        .named("aTimestampUtcMillis")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timestampType(false, MICROS))
        .named("aTimestampNonUtcMicros")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timestampType(true, MICROS))
        .named("aTimestampUtcMicros")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timestampType(false, NANOS))
        .named("aTimestampNonUtcNanos")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timestampType(true, NANOS))
        .named("aTimestampUtcNanos")
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .as(timeType(false, MILLIS))
        .named("aTimeNonUtcMillis")
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .as(timeType(true, MILLIS))
        .named("aTimeUtcMillis")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timeType(false, MICROS))
        .named("aTimeNonUtcMicros")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timeType(true, MICROS))
        .named("aTimeUtcMicros")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timeType(false, NANOS))
        .named("aTimeNonUtcNanos")
        .required(PrimitiveType.PrimitiveTypeName.INT64)
        .as(timeType(true, NANOS))
        .named("aTimeUtcNanos")
        .named("Message");
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
  }

  @Test
  public void testLogicalToConvertedTypeConversion() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();

    assertEquals(ConvertedType.UTF8, parquetMetadataConverter.convertToConvertedType(stringType()));
    assertEquals(ConvertedType.ENUM, parquetMetadataConverter.convertToConvertedType(enumType()));

    assertEquals(ConvertedType.INT_8, parquetMetadataConverter.convertToConvertedType(intType(8, true)));
    assertEquals(ConvertedType.INT_16, parquetMetadataConverter.convertToConvertedType(intType(16, true)));
    assertEquals(ConvertedType.INT_32, parquetMetadataConverter.convertToConvertedType(intType(32, true)));
    assertEquals(ConvertedType.INT_64, parquetMetadataConverter.convertToConvertedType(intType(64, true)));
    assertEquals(ConvertedType.UINT_8, parquetMetadataConverter.convertToConvertedType(intType(8, false)));
    assertEquals(ConvertedType.UINT_16, parquetMetadataConverter.convertToConvertedType(intType(16, false)));
    assertEquals(ConvertedType.UINT_32, parquetMetadataConverter.convertToConvertedType(intType(32, false)));
    assertEquals(ConvertedType.UINT_64, parquetMetadataConverter.convertToConvertedType(intType(64, false)));
    assertEquals(ConvertedType.DECIMAL, parquetMetadataConverter.convertToConvertedType(decimalType(8, 16)));

    assertEquals(
        ConvertedType.TIMESTAMP_MILLIS,
        parquetMetadataConverter.convertToConvertedType(timestampType(true, MILLIS)));
    assertEquals(
        ConvertedType.TIMESTAMP_MICROS,
        parquetMetadataConverter.convertToConvertedType(timestampType(true, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timestampType(true, NANOS)));
    assertEquals(
        ConvertedType.TIMESTAMP_MILLIS,
        parquetMetadataConverter.convertToConvertedType(timestampType(false, MILLIS)));
    assertEquals(
        ConvertedType.TIMESTAMP_MICROS,
        parquetMetadataConverter.convertToConvertedType(timestampType(false, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timestampType(false, NANOS)));

    assertEquals(
        ConvertedType.TIME_MILLIS, parquetMetadataConverter.convertToConvertedType(timeType(true, MILLIS)));
    assertEquals(
        ConvertedType.TIME_MICROS, parquetMetadataConverter.convertToConvertedType(timeType(true, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timeType(true, NANOS)));
    assertEquals(
        ConvertedType.TIME_MILLIS, parquetMetadataConverter.convertToConvertedType(timeType(false, MILLIS)));
    assertEquals(
        ConvertedType.TIME_MICROS, parquetMetadataConverter.convertToConvertedType(timeType(false, MICROS)));
    assertNull(parquetMetadataConverter.convertToConvertedType(timeType(false, NANOS)));

    assertEquals(ConvertedType.DATE, parquetMetadataConverter.convertToConvertedType(dateType()));

    assertEquals(
        ConvertedType.INTERVAL,
        parquetMetadataConverter.convertToConvertedType(
            LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance()));
    assertEquals(ConvertedType.JSON, parquetMetadataConverter.convertToConvertedType(jsonType()));
    assertEquals(ConvertedType.BSON, parquetMetadataConverter.convertToConvertedType(bsonType()));

    assertNull(parquetMetadataConverter.convertToConvertedType(uuidType()));

    assertEquals(ConvertedType.LIST, parquetMetadataConverter.convertToConvertedType(listType()));
    assertEquals(ConvertedType.MAP, parquetMetadataConverter.convertToConvertedType(mapType()));
    assertEquals(
        ConvertedType.MAP_KEY_VALUE,
        parquetMetadataConverter.convertToConvertedType(
            LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance()));
  }

  @Test
  public void testMapLogicalType() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    MessageType expected = Types.buildMessage()
        .requiredGroup()
        .as(mapType())
        .repeatedGroup()
        .as(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(stringType())
        .named("key")
        .required(PrimitiveType.PrimitiveTypeName.INT32)
        .named("value")
        .named("key_value")
        .named("testMap")
        .named("Message");

    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    assertEquals(5, parquetSchema.size());
    assertEquals(new SchemaElement("Message").setNum_children(1), parquetSchema.get(0));
    assertEquals(
        new SchemaElement("testMap")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setNum_children(1)
            .setConverted_type(ConvertedType.MAP)
            .setLogicalType(LogicalType.MAP(new MapType())),
        parquetSchema.get(1));
    // PARQUET-1879 ensure that LogicalType is not written (null) but ConvertedType is MAP_KEY_VALUE for
    // backwards-compatibility
    assertEquals(
        new SchemaElement("key_value")
            .setRepetition_type(FieldRepetitionType.REPEATED)
            .setNum_children(2)
            .setConverted_type(ConvertedType.MAP_KEY_VALUE)
            .setLogicalType(null),
        parquetSchema.get(2));
    assertEquals(
        new SchemaElement("key")
            .setType(Type.BYTE_ARRAY)
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setConverted_type(ConvertedType.UTF8)
            .setLogicalType(LogicalType.STRING(new StringType())),
        parquetSchema.get(3));
    assertEquals(
        new SchemaElement("value")
            .setType(Type.INT32)
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setConverted_type(null)
            .setLogicalType(null),
        parquetSchema.get(4));

    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
  }

  @Test
  public void testVariantLogicalType() {
    byte specVersion = 1;
    MessageType expected = Types.buildMessage()
        .requiredGroup()
        .as(variantType(specVersion))
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .named("value")
        .named("v")
        .named("example");

    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertEquals(expected, schema);
    LogicalTypeAnnotation logicalType = schema.getType("v").getLogicalTypeAnnotation();
    assertEquals(LogicalTypeAnnotation.variantType(specVersion), logicalType);
    assertEquals(specVersion, ((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) logicalType).getSpecVersion());
  }

  @Test
  public void testGeometryLogicalType() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();

    // Create schema with geometry type
    MessageType schema = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType("EPSG:4326"))
        .named("geomField")
        .named("Message");

    // Convert to parquet schema and back
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);

    // Verify the logical type is preserved
    assertEquals(schema, actual);

    PrimitiveType primitiveType = actual.getType("geomField").asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    assertTrue(logicalType instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation);
    assertEquals("EPSG:4326", ((LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) logicalType).getCrs());
  }

  @Test
  public void testGeographyLogicalType() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();

    // Create schema with geography type
    MessageType schema = Types.buildMessage()
        .required(PrimitiveType.PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", EdgeInterpolationAlgorithm.SPHERICAL))
        .named("geogField")
        .named("Message");

    // Convert to parquet schema and back
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);

    // Verify the logical type is preserved
    assertEquals(schema, actual);

    PrimitiveType primitiveType = actual.getType("geogField").asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    assertTrue(logicalType instanceof LogicalTypeAnnotation.GeographyLogicalTypeAnnotation);

    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyType =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) logicalType;
    assertEquals("EPSG:4326", geographyType.getCrs());
    assertEquals(EdgeInterpolationAlgorithm.SPHERICAL, geographyType.getAlgorithm());
  }

  @Test
  public void testGeometryLogicalTypeWithMissingCrs() {
    // Create a Geometry logical type without specifying CRS
    GeometryType geometryType = new GeometryType();
    LogicalType logicalType = new LogicalType();
    logicalType.setGEOMETRY(geometryType);

    // Convert to LogicalTypeAnnotation
    ParquetSchemaConverter converter = new ParquetSchemaConverter();
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertNotNull("Geometry annotation should not be null", annotation);
    assertTrue(
        "Should be a GeometryLogicalTypeAnnotation",
        annotation instanceof LogicalTypeAnnotation.GeometryLogicalTypeAnnotation);

    LogicalTypeAnnotation.GeometryLogicalTypeAnnotation geometryAnnotation =
        (LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) annotation;

    // Default behavior should use null or empty CRS
    assertNull("CRS should be null or empty when not specified", geometryAnnotation.getCrs());
  }

  @Test
  public void testGeographyLogicalTypeWithMissingParameters() {
    ParquetSchemaConverter converter = new ParquetSchemaConverter();

    // Create a Geography logical type without CRS and algorithm
    GeographyType geographyType = new GeographyType();
    LogicalType logicalType = new LogicalType();
    logicalType.setGEOGRAPHY(geographyType);

    // Convert to LogicalTypeAnnotation
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertNotNull("Geography annotation should not be null", annotation);
    assertTrue(
        "Should be a GeographyLogicalTypeAnnotation",
        annotation instanceof LogicalTypeAnnotation.GeographyLogicalTypeAnnotation);

    // Check that optional parameters are handled correctly
    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyAnnotation =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) annotation;
    assertNull("CRS should be null when not specified", geographyAnnotation.getCrs());
    // Most implementations default to LINEAR when algorithm is not specified
    assertNull("Algorithm should be null when not specified", geographyAnnotation.getAlgorithm());

    // Now test the round-trip conversion
    LogicalType roundTripType = converter.convertToLogicalType(annotation);
    assertEquals("setField should be GEOGRAPHY", LogicalType._Fields.GEOGRAPHY, roundTripType.getSetField());
    assertNull(
        "Round trip CRS should still be null",
        roundTripType.getGEOGRAPHY().getCrs());
    assertNull(
        "Round trip Algorithm should be null",
        roundTripType.getGEOGRAPHY().getAlgorithm());
  }

  @Test
  public void testGeographyLogicalTypeWithAlgorithmButNoCrs() {
    // Create a Geography logical type with algorithm but no CRS
    GeographyType geographyType = new GeographyType();
    geographyType.setAlgorithm(org.apache.parquet.format.EdgeInterpolationAlgorithm.SPHERICAL);
    LogicalType logicalType = new LogicalType();
    logicalType.setGEOGRAPHY(geographyType);

    // Convert to LogicalTypeAnnotation
    ParquetSchemaConverter converter = new ParquetSchemaConverter();
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    Assert.assertNotNull("Geography annotation should not be null", annotation);
    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyAnnotation =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) annotation;

    // CRS should be null/empty but algorithm should be set
    assertNull("CRS should be null or empty", geographyAnnotation.getCrs());
    assertEquals(
        "Algorithm should be SPHERICAL",
        EdgeInterpolationAlgorithm.SPHERICAL,
        geographyAnnotation.getAlgorithm());
  }
}
