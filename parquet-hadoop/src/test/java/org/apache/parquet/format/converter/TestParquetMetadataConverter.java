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
package org.apache.parquet.format.converter;

import static java.util.Collections.emptyList;
import static org.apache.parquet.format.CompressionCodec.UNCOMPRESSED;
import static org.apache.parquet.format.Type.INT32;
import static org.apache.parquet.format.Util.readPageHeader;
import static org.apache.parquet.format.Util.writePageHeader;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaDataByMidpoint;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.filterFileMetaDataByStart;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.getOffset;
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
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.data.Offset.offset;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.FixedBinaryTestUtils;
import org.apache.parquet.Version;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.SizeStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.statistics.geospatial.GeospatialTypes;
import org.apache.parquet.crypto.DecryptionPropertiesFactory;
import org.apache.parquet.crypto.EncryptionPropertiesFactory;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.crypto.InternalFileDecryptor;
import org.apache.parquet.example.Paper;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.format.BoundingBox;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.DecimalType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.GeographyType;
import org.apache.parquet.format.GeometryType;
import org.apache.parquet.format.GeospatialStatistics;
import org.apache.parquet.format.LogicalType;
import org.apache.parquet.format.MapType;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.StringType;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.ColumnIndexBuilder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestParquetMetadataConverter {
  private static SecureRandom random = new SecureRandom();
  private static final String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
  private static final String CHAR_UPPER = CHAR_LOWER.toUpperCase();
  private static final String NUMBER = "0123456789";
  private static final String DATA_FOR_RANDOM_STRING = CHAR_LOWER + CHAR_UPPER + NUMBER;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testPageHeader() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    PageType type = PageType.DATA_PAGE;
    int compSize = 10;
    int uncSize = 20;
    PageHeader pageHeader = new PageHeader(type, uncSize, compSize);
    writePageHeader(pageHeader, out);
    PageHeader readPageHeader = readPageHeader(new ByteArrayInputStream(out.toByteArray()));
    assertThat(readPageHeader).isEqualTo(pageHeader);
  }

  @Test
  public void testSchemaConverter() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(Paper.schema);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertThat(schema).isEqualTo(Paper.schema);
  }

  @Test
  public void testSchemaConverterDecimal() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> schemaElements = parquetMetadataConverter.toParquetSchema(Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinaryDecimal")
        .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(4)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aFixedDecimal")
        .named("Message"));
    List<SchemaElement> expected = Lists.newArrayList(
        new SchemaElement("Message").setNum_children(2),
        new SchemaElement("aBinaryDecimal")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setType(Type.BYTE_ARRAY)
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
    assertThat(schemaElements).isEqualTo(expected);
  }

  @Test
  public void testParquetMetadataConverterWithDictionary() throws IOException {
    ParquetMetadata parquetMetaData = createParquetMetaData(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN);
    testParquetMetadataConverterWithDictionary(parquetMetaData);
  }

  @Test
  public void testParquetMetadataConverterWithDictionaryAndWithoutEncodingStats() throws IOException {
    ParquetMetadata parquetMetaData = createParquetMetaData(Encoding.PLAIN_DICTIONARY, Encoding.PLAIN, false);
    testParquetMetadataConverterWithDictionary(parquetMetaData);
  }

  private void testParquetMetadataConverterWithDictionary(ParquetMetadata parquetMetaData) throws IOException {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData fmd1 = converter.toParquetMetadata(1, parquetMetaData);

    // Flag should be true
    fmd1.row_groups.forEach(rowGroup -> rowGroup.columns.forEach(column -> {
      assertThat(column.meta_data.isSetDictionary_page_offset()).isTrue();
    }));

    ByteArrayOutputStream metaDataOutputStream = new ByteArrayOutputStream();
    Util.writeFileMetaData(fmd1, metaDataOutputStream);
    ByteArrayInputStream metaDataInputStream = new ByteArrayInputStream(metaDataOutputStream.toByteArray());
    FileMetaData fmd2 = Util.readFileMetaData(metaDataInputStream);
    ParquetMetadata parquetMetaDataConverted = converter.fromParquetMetadata(fmd2);

    long dicOffsetOriginal =
        parquetMetaData.getBlocks().get(0).getColumns().get(0).getDictionaryPageOffset();
    long dicOffsetConverted =
        parquetMetaDataConverted.getBlocks().get(0).getColumns().get(0).getDictionaryPageOffset();

    assertThat(dicOffsetConverted).isEqualTo(dicOffsetOriginal);
  }

  @Test
  public void testParquetMetadataConverterWithoutDictionary() throws IOException {
    ParquetMetadata parquetMetaData = createParquetMetaData(null, Encoding.PLAIN);

    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData fmd1 = converter.toParquetMetadata(1, parquetMetaData);

    // Flag should be false
    fmd1.row_groups.forEach(rowGroup -> rowGroup.columns.forEach(column -> {
      assertThat(column.meta_data.isSetDictionary_page_offset()).isFalse();
    }));

    ByteArrayOutputStream metaDataOutputStream = new ByteArrayOutputStream();
    Util.writeFileMetaData(fmd1, metaDataOutputStream);
    ByteArrayInputStream metaDataInputStream = new ByteArrayInputStream(metaDataOutputStream.toByteArray());
    FileMetaData fmd2 = Util.readFileMetaData(metaDataInputStream);
    ParquetMetadata pmd2 = converter.fromParquetMetadata(fmd2);

    long dicOffsetConverted = pmd2.getBlocks().get(0).getColumns().get(0).getDictionaryPageOffset();

    assertThat(dicOffsetConverted).isEqualTo(0);
  }

  @Test
  public void testBloomFilterOffset() throws IOException {
    ParquetMetadata origMetaData = createParquetMetaData(null, Encoding.PLAIN);
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Without bloom filter offset
    FileMetaData footer = converter.toParquetMetadata(1, origMetaData);
    assertThat(footer.getRow_groups()
            .get(0)
            .getColumns()
            .get(0)
            .getMeta_data()
            .isSetBloom_filter_offset())
        .isFalse();
    ParquetMetadata convertedMetaData = converter.fromParquetMetadata(footer);
    assertThat(convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterOffset())
        .isNegative();

    // With bloom filter offset
    origMetaData.getBlocks().get(0).getColumns().get(0).setBloomFilterOffset(1234);
    footer = converter.toParquetMetadata(1, origMetaData);
    assertThat(footer.getRow_groups()
            .get(0)
            .getColumns()
            .get(0)
            .getMeta_data()
            .isSetBloom_filter_offset())
        .isTrue();
    assertThat(footer.getRow_groups()
            .get(0)
            .getColumns()
            .get(0)
            .getMeta_data()
            .getBloom_filter_offset())
        .isEqualTo(1234);
    convertedMetaData = converter.fromParquetMetadata(footer);
    assertThat(convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterOffset())
        .isEqualTo(1234);
  }

  @Test
  public void testBloomFilterLength() throws IOException {
    ParquetMetadata origMetaData = createParquetMetaData(null, Encoding.PLAIN);
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Without bloom filter length
    FileMetaData footer = converter.toParquetMetadata(1, origMetaData);
    assertThat(footer.getRow_groups()
            .get(0)
            .getColumns()
            .get(0)
            .getMeta_data()
            .isSetBloom_filter_length())
        .isFalse();
    ParquetMetadata convertedMetaData = converter.fromParquetMetadata(footer);
    assertThat(convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterLength())
        .isNegative();

    // With bloom filter length
    origMetaData.getBlocks().get(0).getColumns().get(0).setBloomFilterLength(1024);
    footer = converter.toParquetMetadata(1, origMetaData);
    assertThat(footer.getRow_groups()
            .get(0)
            .getColumns()
            .get(0)
            .getMeta_data()
            .isSetBloom_filter_length())
        .isTrue();
    assertThat(footer.getRow_groups()
            .get(0)
            .getColumns()
            .get(0)
            .getMeta_data()
            .getBloom_filter_length())
        .isEqualTo(1024);
    convertedMetaData = converter.fromParquetMetadata(footer);
    assertThat(convertedMetaData.getBlocks().get(0).getColumns().get(0).getBloomFilterLength())
        .isEqualTo(1024);
  }

  @Test
  public void testLogicalTypesBackwardCompatibleWithConvertedTypes() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType expected = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
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
    assertThat(schema).isEqualTo(expected);
  }

  @Test
  public void testUnknownLogicalTypePreservesPhysicalType() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    // The generated Thrift reader skips an unknown union member, leaving the union unset.
    LogicalType unknownLogicalType = new LogicalType();
    List<SchemaElement> parquetSchema = Lists.newArrayList(
        new SchemaElement("Message").setNum_children(1),
        new SchemaElement("unknown")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setType(Type.BYTE_ARRAY)
            .setLogicalType(unknownLogicalType));

    MessageType schema = converter.fromParquetSchema(parquetSchema, null);

    PrimitiveType unknown = schema.getType("unknown").asPrimitiveType();
    assertThat(unknown.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.BINARY);
    assertThat(unknown.getLogicalTypeAnnotation()).isNull();
  }

  @Test
  public void testUnknownLogicalTypeUsesConvertedTypeFallback() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    LogicalType unknownLogicalType = new LogicalType();
    // Use DECIMAL to verify that converted-type precision and scale are preserved.
    List<SchemaElement> parquetSchema = Lists.newArrayList(
        new SchemaElement("Message").setNum_children(1),
        new SchemaElement("unknownWithConvertedType")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setType(Type.BYTE_ARRAY)
            .setLogicalType(unknownLogicalType)
            .setConverted_type(ConvertedType.DECIMAL)
            .setPrecision(9)
            .setScale(2));

    MessageType schema = converter.fromParquetSchema(parquetSchema, null);

    PrimitiveType unknownWithConvertedType =
        schema.getType("unknownWithConvertedType").asPrimitiveType();
    assertThat(unknownWithConvertedType.getPrimitiveTypeName()).isEqualTo(PrimitiveTypeName.BINARY);
    assertThat(unknownWithConvertedType.getLogicalTypeAnnotation()).isEqualTo(decimalType(2, 9));
  }

  @Test
  public void testIncompatibleLogicalAndConvertedTypes() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(OriginalType.DECIMAL)
        .precision(9)
        .scale(2)
        .named("aBinary")
        .named("Message");
    MessageType expected = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.jsonType())
        .named("aBinary")
        .named("Message");

    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    // Set converted type field to a different type to verify that in case of mismatch, it overrides logical type
    parquetSchema.get(1).setConverted_type(ConvertedType.JSON);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testTimeLogicalTypes() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType expected = Types.buildMessage()
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(false, MILLIS))
        .named("aTimestampNonUtcMillis")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(true, MILLIS))
        .named("aTimestampUtcMillis")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(false, MICROS))
        .named("aTimestampNonUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(true, MICROS))
        .named("aTimestampUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(false, NANOS))
        .named("aTimestampNonUtcNanos")
        .required(PrimitiveTypeName.INT64)
        .as(timestampType(true, NANOS))
        .named("aTimestampUtcNanos")
        .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(timestampType(true, MILLIS))
        .named("aTimestampFlbaMillis")
        .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(timestampType(true, MICROS))
        .named("aTimestampFlbaMicros")
        .required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(12)
        .as(timestampType(true, NANOS))
        .named("aTimestampFlbaNanos")
        .required(PrimitiveTypeName.INT32)
        .as(timeType(false, MILLIS))
        .named("aTimeNonUtcMillis")
        .required(PrimitiveTypeName.INT32)
        .as(timeType(true, MILLIS))
        .named("aTimeUtcMillis")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(false, MICROS))
        .named("aTimeNonUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(true, MICROS))
        .named("aTimeUtcMicros")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(false, NANOS))
        .named("aTimeNonUtcNanos")
        .required(PrimitiveTypeName.INT64)
        .as(timeType(true, NANOS))
        .named("aTimeUtcNanos")
        .named("Message");
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    // FLBA(12) MILLIS/MICROS must not write a legacy converted_type (it is INT64-only).
    SchemaElement flbaMillis = parquetSchema.stream()
        .filter(e -> "aTimestampFlbaMillis".equals(e.getName()))
        .findFirst()
        .get();
    assertThat(flbaMillis.isSetConverted_type()).isFalse();
    assertThat(flbaMillis.isSetLogicalType()).isTrue();
    SchemaElement flbaMicros = parquetSchema.stream()
        .filter(e -> "aTimestampFlbaMicros".equals(e.getName()))
        .findFirst()
        .get();
    assertThat(flbaMicros.isSetConverted_type()).isFalse();
    assertThat(flbaMicros.isSetLogicalType()).isTrue();
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertThat(schema).isEqualTo(expected);
  }

  @Test
  public void testLogicalToConvertedTypeConversion() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    assertThat(parquetMetadataConverter.convertToConvertedType(stringType()))
        .isEqualTo(ConvertedType.UTF8);
    assertThat(parquetMetadataConverter.convertToConvertedType(enumType())).isEqualTo(ConvertedType.ENUM);

    assertThat(parquetMetadataConverter.convertToConvertedType(intType(8, true)))
        .isEqualTo(ConvertedType.INT_8);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(16, true)))
        .isEqualTo(ConvertedType.INT_16);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(32, true)))
        .isEqualTo(ConvertedType.INT_32);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(64, true)))
        .isEqualTo(ConvertedType.INT_64);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(8, false)))
        .isEqualTo(ConvertedType.UINT_8);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(16, false)))
        .isEqualTo(ConvertedType.UINT_16);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(32, false)))
        .isEqualTo(ConvertedType.UINT_32);
    assertThat(parquetMetadataConverter.convertToConvertedType(intType(64, false)))
        .isEqualTo(ConvertedType.UINT_64);
    assertThat(parquetMetadataConverter.convertToConvertedType(decimalType(8, 16)))
        .isEqualTo(ConvertedType.DECIMAL);

    assertThat(parquetMetadataConverter.convertToConvertedType(timestampType(true, MILLIS)))
        .isEqualTo(ConvertedType.TIMESTAMP_MILLIS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timestampType(true, MICROS)))
        .isEqualTo(ConvertedType.TIMESTAMP_MICROS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timestampType(true, NANOS)))
        .isNull();
    assertThat(parquetMetadataConverter.convertToConvertedType(timestampType(false, MILLIS)))
        .isEqualTo(ConvertedType.TIMESTAMP_MILLIS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timestampType(false, MICROS)))
        .isEqualTo(ConvertedType.TIMESTAMP_MICROS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timestampType(false, NANOS)))
        .isNull();

    assertThat(parquetMetadataConverter.convertToConvertedType(timeType(true, MILLIS)))
        .isEqualTo(ConvertedType.TIME_MILLIS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timeType(true, MICROS)))
        .isEqualTo(ConvertedType.TIME_MICROS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timeType(true, NANOS)))
        .isNull();
    assertThat(parquetMetadataConverter.convertToConvertedType(timeType(false, MILLIS)))
        .isEqualTo(ConvertedType.TIME_MILLIS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timeType(false, MICROS)))
        .isEqualTo(ConvertedType.TIME_MICROS);
    assertThat(parquetMetadataConverter.convertToConvertedType(timeType(false, NANOS)))
        .isNull();

    assertThat(parquetMetadataConverter.convertToConvertedType(dateType())).isEqualTo(ConvertedType.DATE);

    assertThat(parquetMetadataConverter.convertToConvertedType(
            LogicalTypeAnnotation.IntervalLogicalTypeAnnotation.getInstance()))
        .isEqualTo(ConvertedType.INTERVAL);
    assertThat(parquetMetadataConverter.convertToConvertedType(jsonType())).isEqualTo(ConvertedType.JSON);
    assertThat(parquetMetadataConverter.convertToConvertedType(bsonType())).isEqualTo(ConvertedType.BSON);

    assertThat(parquetMetadataConverter.convertToConvertedType(uuidType())).isNull();

    assertThat(parquetMetadataConverter.convertToConvertedType(listType())).isEqualTo(ConvertedType.LIST);
    assertThat(parquetMetadataConverter.convertToConvertedType(mapType())).isEqualTo(ConvertedType.MAP);
    assertThat(parquetMetadataConverter.convertToConvertedType(
            LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance()))
        .isEqualTo(ConvertedType.MAP_KEY_VALUE);
  }

  @Test
  public void testEnumEquivalence() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    for (org.apache.parquet.column.Encoding encoding : org.apache.parquet.column.Encoding.values()) {
      assertThat(parquetMetadataConverter.getEncoding(parquetMetadataConverter.getEncoding(encoding)))
          .isEqualTo(encoding);
    }
    for (org.apache.parquet.format.Encoding encoding : org.apache.parquet.format.Encoding.values()) {
      assertThat(parquetMetadataConverter.getEncoding(parquetMetadataConverter.getEncoding(encoding)))
          .isEqualTo(encoding);
    }
    for (Repetition repetition : Repetition.values()) {
      assertThat(parquetMetadataConverter.fromParquetRepetition(
              parquetMetadataConverter.toParquetRepetition(repetition)))
          .isEqualTo(repetition);
    }
    for (FieldRepetitionType repetition : FieldRepetitionType.values()) {
      assertThat(parquetMetadataConverter.toParquetRepetition(
              parquetMetadataConverter.fromParquetRepetition(repetition)))
          .isEqualTo(repetition);
    }
    for (PrimitiveTypeName primitiveTypeName : PrimitiveTypeName.values()) {
      assertThat(parquetMetadataConverter.getPrimitive(parquetMetadataConverter.getType(primitiveTypeName)))
          .isEqualTo(primitiveTypeName);
    }
    for (Type type : Type.values()) {
      assertThat(parquetMetadataConverter.getType(parquetMetadataConverter.getPrimitive(type)))
          .isEqualTo(type);
    }
    for (OriginalType original : OriginalType.values()) {
      assertThat(parquetMetadataConverter
              .getLogicalTypeAnnotation(
                  parquetMetadataConverter.convertToConvertedType(
                      LogicalTypeAnnotation.fromOriginalType(original, null)),
                  null)
              .toOriginalType())
          .isEqualTo(original);
    }
    for (ConvertedType converted : ConvertedType.values()) {
      assertThat(parquetMetadataConverter.convertToConvertedType(
              parquetMetadataConverter.getLogicalTypeAnnotation(converted, null)))
          .isEqualTo(converted);
    }
  }

  private FileMetaData metadata(long... sizes) {
    List<SchemaElement> schema = emptyList();
    List<RowGroup> rowGroups = new ArrayList<RowGroup>();
    long offset = 0;
    for (long size : sizes) {
      ColumnChunk columnChunk = new ColumnChunk(offset);
      columnChunk.setMeta_data(new ColumnMetaData(
          INT32,
          Collections.<org.apache.parquet.format.Encoding>emptyList(),
          Collections.<String>emptyList(),
          UNCOMPRESSED,
          10l,
          size * 2,
          size,
          offset));
      rowGroups.add(new RowGroup(List.of(columnChunk), size, 1));
      offset += size;
    }
    return new FileMetaData(1, schema, sizes.length, rowGroups);
  }

  private FileMetaData filter(FileMetaData md, long start, long end) {
    return filterFileMetaDataByMidpoint(
        new FileMetaData(md), new ParquetMetadataConverter.RangeMetadataFilter(start, end));
  }

  private FileMetaData find(FileMetaData md, Long... blockStart) {
    return filterFileMetaDataByStart(
        new FileMetaData(md),
        new ParquetMetadataConverter.OffsetMetadataFilter(Sets.newHashSet((Long[]) blockStart)));
  }

  private FileMetaData find(FileMetaData md, long blockStart) {
    return filterFileMetaDataByStart(
        new FileMetaData(md), new ParquetMetadataConverter.OffsetMetadataFilter(Sets.newHashSet(blockStart)));
  }

  private void verifyMD(FileMetaData md, long... offsets) {
    assertThat(md.row_groups).hasSameSizeAs(offsets);
    for (int i = 0; i < offsets.length; i++) {
      long offset = offsets[i];
      RowGroup rowGroup = md.getRow_groups().get(i);
      assertThat(getOffset(rowGroup)).isEqualTo(offset);
    }
  }

  /**
   * verifies that splits will end up being a partition of the rowgroup
   * they are all found only once
   *
   * @param md
   * @param splitWidth
   */
  private void verifyAllFilters(FileMetaData md, long splitWidth) {
    Set<Long> offsetsFound = new TreeSet<Long>();
    for (long start = 0; start < fileSize(md); start += splitWidth) {
      FileMetaData filtered = filter(md, start, start + splitWidth);
      for (RowGroup rg : filtered.getRow_groups()) {
        long o = getOffset(rg);
        assertThat(offsetsFound).as("found the offset twice: " + o).doesNotContain(o);
        offsetsFound.add(o);
      }
    }
    assertThat(offsetsFound)
        .as("found: " + offsetsFound + "\nexpected " + md.getRow_groups())
        .hasSize(md.getRow_groups().size());
  }

  private long fileSize(FileMetaData md) {
    long size = 0;
    for (RowGroup rg : md.getRow_groups()) {
      size += rg.total_byte_size;
    }
    return size;
  }

  @Test
  public void testFilterMetaData() {
    verifyMD(filter(metadata(50, 50, 50), 0, 50), 0);
    verifyMD(filter(metadata(50, 50, 50), 50, 100), 50);
    verifyMD(filter(metadata(50, 50, 50), 100, 150), 100);
    // picks up first RG
    verifyMD(filter(metadata(50, 50, 50), 25, 75), 0);
    // picks up no RG
    verifyMD(filter(metadata(50, 50, 50), 26, 75));
    // picks up second RG
    verifyMD(filter(metadata(50, 50, 50), 26, 76), 50);

    verifyAllFilters(metadata(50, 50, 50), 10);
    verifyAllFilters(metadata(50, 50, 50), 51);
    verifyAllFilters(metadata(50, 50, 50), 25); // corner cases are in the middle
    verifyAllFilters(metadata(50, 50, 50), 24);
    verifyAllFilters(metadata(50, 50, 50), 26);
    verifyAllFilters(metadata(50, 50, 50), 110);
    verifyAllFilters(metadata(10, 50, 500), 110);
    verifyAllFilters(metadata(10, 50, 500), 10);
    verifyAllFilters(metadata(10, 50, 500), 600);
    verifyAllFilters(metadata(11, 9, 10), 10);
    verifyAllFilters(metadata(11, 9, 10), 9);
    verifyAllFilters(metadata(11, 9, 10), 8);
  }

  @Test
  public void testFindRowGroups() {
    verifyMD(find(metadata(50, 50, 50), 0), 0);
    verifyMD(find(metadata(50, 50, 50), 50), 50);
    verifyMD(find(metadata(50, 50, 50), 100), 100);
    verifyMD(find(metadata(50, 50, 50), 0L, 50L), 0, 50);
    verifyMD(find(metadata(50, 50, 50), 0L, 50L, 100L), 0, 50, 100);
    verifyMD(find(metadata(50, 50, 50), 50L, 100L), 50, 100);
    // doesn't find an offset that isn't the start of a row group.
    verifyMD(find(metadata(50, 50, 50), 10));
  }

  @Test
  public void randomTestFilterMetaData() {
    // randomized property based testing
    // if it fails add the case above
    Random random = new Random(42);
    for (int j = 0; j < 100; j++) {
      long[] rgs = new long[random.nextInt(50)];
      for (int i = 0; i < rgs.length; i++) {
        rgs[i] = random.nextInt(10000) + 1; // No empty row groups
      }
      int splitSize = random.nextInt(10000) + 1; // 0 would lead to an infinite loop
      try {
        verifyAllFilters(metadata(rgs), splitSize);
      } catch (AssertionError e) {
        throw (AssertionError) new AssertionError(
                "fail verifyAllFilters(metadata(" + Arrays.toString(rgs) + "), " + splitSize + ")")
            .initCause(e);
      }
    }
  }

  @Test
  public void testFieldMetadataDebugLogging() {
    MessageType schema = parseMessageType("message test { optional binary some_null_field; }");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(
            schema,
            new HashMap<>(),
            null,
            org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType.UNENCRYPTED,
            null);
    List<BlockMetaData> blockMetaDataList = new ArrayList<>();
    BlockMetaData blockMetaData = new BlockMetaData();
    blockMetaData.addColumn(createColumnChunkMetaData());
    blockMetaDataList.add(blockMetaData);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, blockMetaDataList);
    ParquetMetadata.toJSON(metadata);
  }

  @Test
  public void testEncryptedFieldMetadataDebugLogging() {
    Configuration conf = new Configuration();
    conf.set(
        EncryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME,
        "org.apache.parquet.crypto.SampleDecryptionPropertiesFactory");
    DecryptionPropertiesFactory decryptionPropertiesFactory = DecryptionPropertiesFactory.loadFactory(conf);
    FileDecryptionProperties decryptionProperties =
        decryptionPropertiesFactory.getFileDecryptionProperties(conf, null);

    MessageType schema = parseMessageType("message test { optional binary some_null_field; }");

    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(
            schema,
            new HashMap<>(),
            null,
            org.apache.parquet.hadoop.metadata.FileMetaData.EncryptionType.ENCRYPTED_FOOTER,
            new InternalFileDecryptor(decryptionProperties));

    List<BlockMetaData> blockMetaDataList = new ArrayList<>();
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, blockMetaDataList);
    ParquetMetadata.toJSON(metadata);
    System.out.println(ParquetMetadata.toPrettyJSON(metadata));
  }

  @Test
  public void testMetadataToJson() {
    ParquetMetadata metadata = new ParquetMetadata(null, null);
    assertThat(ParquetMetadata.toJSON(metadata)).isEqualTo("{\"fileMetaData\":null,\"blocks\":null}");
    assertThat(ParquetMetadata.toPrettyJSON(metadata))
        .isEqualTo(("{\n" + "  \"fileMetaData\" : null,\n" + "  \"blocks\" : null\n" + "}")
            .replace("\n", System.lineSeparator()));
  }

  private ColumnChunkMetaData createColumnChunkMetaData() {
    Set<org.apache.parquet.column.Encoding> e = new HashSet<org.apache.parquet.column.Encoding>();
    PrimitiveTypeName t = PrimitiveTypeName.BINARY;
    ColumnPath p = ColumnPath.get("foo");
    CompressionCodecName c = CompressionCodecName.GZIP;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, e, s, 0, 0, 0, 0, 0);
    return md;
  }

  @Test
  public void testEncodingsCache() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    List<org.apache.parquet.format.Encoding> formatEncodingsCopy1 = List.of(
        org.apache.parquet.format.Encoding.BIT_PACKED,
        org.apache.parquet.format.Encoding.RLE_DICTIONARY,
        org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    List<org.apache.parquet.format.Encoding> formatEncodingsCopy2 = List.of(
        org.apache.parquet.format.Encoding.BIT_PACKED,
        org.apache.parquet.format.Encoding.RLE_DICTIONARY,
        org.apache.parquet.format.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    Set<org.apache.parquet.column.Encoding> expected = new HashSet<org.apache.parquet.column.Encoding>();
    expected.add(org.apache.parquet.column.Encoding.BIT_PACKED);
    expected.add(org.apache.parquet.column.Encoding.RLE_DICTIONARY);
    expected.add(org.apache.parquet.column.Encoding.DELTA_LENGTH_BYTE_ARRAY);

    Set<org.apache.parquet.column.Encoding> res1 =
        parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res2 =
        parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy1);
    Set<org.apache.parquet.column.Encoding> res3 =
        parquetMetadataConverter.fromFormatEncodings(formatEncodingsCopy2);

    // make sure they are all semantically equal
    assertThat(res1).isEqualTo(expected);
    assertThat(res2).isEqualTo(expected);
    assertThat(res3).isEqualTo(expected);

    // make sure res1, res2, and res3 are actually the same cached object
    assertThat(res2).isSameAs(res1);
    assertThat(res3).isSameAs(res1);

    // make sure they are all unmodifiable (UnmodifiableSet is not public, so we have to compare on class name)
    assertThat(res1.getClass().getName()).isEqualTo("java.util.Collections$UnmodifiableSet");
    assertThat(res2.getClass().getName()).isEqualTo("java.util.Collections$UnmodifiableSet");
    assertThat(res3.getClass().getName()).isEqualTo("java.util.Collections$UnmodifiableSet");
  }

  @Test
  public void testEncodingsOrder() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    Set<org.apache.parquet.column.Encoding> columnEncodings =
        new HashSet<>(Arrays.asList(org.apache.parquet.column.Encoding.values()));

    // Assert that the encodings are returned in ascending ordinal order
    List<org.apache.parquet.format.Encoding> formatEncodings =
        parquetMetadataConverter.toFormatEncodings(columnEncodings);
    for (int i = 1; i < formatEncodings.size(); i++) {
      assertThat(formatEncodings.get(i - 1).ordinal())
          .isLessThan(formatEncodings.get(i).ordinal());
    }
  }

  @Test
  public void testBinaryStatsV1() {
    testBinaryStats(StatsHelper.V1);
  }

  @Test
  public void testBinaryStatsV2() {
    testBinaryStats(StatsHelper.V2);
  }

  private void testBinaryStats(StatsHelper helper) {
    // make fake stats and verify the size check
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls(3004);
    byte[] min = new byte[904];
    byte[] max = new byte[2388];
    stats.updateStats(Binary.fromConstantByteArray(min));
    stats.updateStats(Binary.fromConstantByteArray(max));
    long totalLen = min.length + max.length;
    assertThat(stats.isSmallerThan(totalLen))
        .as("Should not be smaller than min + max size")
        .isFalse();
    assertThat(stats.isSmallerThan(totalLen + 1))
        .as("Should be smaller than min + max size + 1")
        .isTrue();

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertThat(formatStats.isSetMin()).as("Min should not be set").isFalse();
    assertThat(formatStats.isSetMax()).as("Max should not be set").isFalse();
    if (helper == StatsHelper.V2) {
      assertThat(formatStats.getMin_value()).as("Min_value should match").isEqualTo(min);
      assertThat(formatStats.getMax_value()).as("Max_value should match").isEqualTo(max);
    }
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);

    // min/max are not written because the values are too large, but null count is always written
    stats.setMinMaxFromBytes(max, max);

    formatStats = helper.toParquetStatistics(stats);

    assertThat(formatStats.isSetMin()).as("Min should not be set").isFalse();
    assertThat(formatStats.isSetMax()).as("Max should not be set").isFalse();
    assertThat(formatStats.isSetMin_value())
        .as("Min_value should not be set")
        .isFalse();
    assertThat(formatStats.isSetMax_value())
        .as("Max_value should not be set")
        .isFalse();
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);

    Statistics roundTripStats = ParquetMetadataConverter.fromParquetStatisticsInternal(
        Version.FULL_VERSION,
        formatStats,
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.BINARY, ""),
        ParquetMetadataConverter.SortOrder.SIGNED);

    assertThat(roundTripStats.isEmpty())
        .as("Round-trip stats should not be empty (null count is set)")
        .isFalse();
    assertThat(roundTripStats.getNumNulls())
        .as("Round-trip null count should match")
        .isEqualTo(3004);
  }

  @Test
  public void testBinaryStatsWithTruncation() {
    int defaultTruncLen = ParquetProperties.DEFAULT_STATISTICS_TRUNCATE_LENGTH;
    int[] validLengths = {1, 2, 16, 64, defaultTruncLen - 1};
    for (int len : validLengths) {
      testBinaryStatsWithTruncation(len, 60, 70);
      testBinaryStatsWithTruncation(len, (int) ParquetMetadataConverter.MAX_STATS_SIZE, 190);
      testBinaryStatsWithTruncation(len, 280, (int) ParquetMetadataConverter.MAX_STATS_SIZE);
      testBinaryStatsWithTruncation(
          len, (int) ParquetMetadataConverter.MAX_STATS_SIZE, (int) ParquetMetadataConverter.MAX_STATS_SIZE);
    }

    int[] invalidLengths = {-1, 0, Integer.MAX_VALUE + 1};
    for (int len : invalidLengths) {
      assertThatThrownBy(() -> testBinaryStatsWithTruncation(len, 80, 20))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Truncate length should be greater than 0");
    }
  }

  // The number of minLen and maxLen shouldn't matter because the comparision is controlled by prefix
  private void testBinaryStatsWithTruncation(int truncateLen, int minLen, int maxLen) {
    BinaryStatistics stats = new BinaryStatistics();
    byte[] min = generateRandomString("a", minLen).getBytes();
    byte[] max = generateRandomString("b", maxLen).getBytes();
    stats.updateStats(Binary.fromConstantByteArray(min));
    stats.updateStats(Binary.fromConstantByteArray(max));
    ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter(truncateLen);
    org.apache.parquet.format.Statistics formatStats = metadataConverter.toParquetStatistics(stats);

    if (minLen + maxLen >= ParquetMetadataConverter.MAX_STATS_SIZE) {
      assertThat(formatStats.getMin_value()).isNull();
      assertThat(formatStats.getMax_value()).isNull();
    } else {
      String minString = new String(min, Charset.forName("UTF-8"));
      String minStatString = new String(formatStats.getMin_value(), Charset.forName("UTF-8"));
      assertThat(minStatString.compareTo(minString)).isLessThanOrEqualTo(0);
      String maxString = new String(max, Charset.forName("UTF-8"));
      String maxStatString = new String(formatStats.getMax_value(), Charset.forName("UTF-8"));
      assertThat(maxStatString.compareTo(maxString)).isGreaterThanOrEqualTo(0);
    }
  }

  private static String generateRandomString(String prefix, int length) {
    assertThat(prefix.length()).isLessThanOrEqualTo(length);
    StringBuilder sb = new StringBuilder(length);
    sb.append(prefix);
    for (int i = 0; i < length - prefix.length(); i++) {
      int rndCharAt = random.nextInt(DATA_FOR_RANDOM_STRING.length());
      char rndChar = DATA_FOR_RANDOM_STRING.charAt(rndCharAt);
      sb.append(rndChar);
    }
    return sb.toString();
  }

  @Test
  public void testIntegerStatsV1() {
    testIntegerStats(StatsHelper.V1);
  }

  @Test
  public void testIntegerStatsV2() {
    testIntegerStats(StatsHelper.V2);
  }

  private void testIntegerStats(StatsHelper helper) {
    // make fake stats and verify the size check
    IntStatistics stats = new IntStatistics();
    stats.incrementNumNulls(3004);
    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertThat(BytesUtils.bytesToInt(formatStats.getMin()))
        .as("Min should match")
        .isEqualTo(min);
    assertThat(BytesUtils.bytesToInt(formatStats.getMax()))
        .as("Max should match")
        .isEqualTo(max);
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);
  }

  @Test
  public void testLongStatsV1() {
    testLongStats(StatsHelper.V1);
  }

  @Test
  public void testLongStatsV2() {
    testLongStats(StatsHelper.V2);
  }

  private void testLongStats(StatsHelper helper) {
    // make fake stats and verify the size check
    LongStatistics stats = new LongStatistics();
    stats.incrementNumNulls(3004);
    long min = Long.MIN_VALUE;
    long max = Long.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertThat(BytesUtils.bytesToLong(formatStats.getMin()))
        .as("Min should match")
        .isEqualTo(min);
    assertThat(BytesUtils.bytesToLong(formatStats.getMax()))
        .as("Max should match")
        .isEqualTo(max);
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);
  }

  @Test
  public void testFloatStatsV1() {
    testFloatStats(StatsHelper.V1);
  }

  @Test
  public void testFloatStatsV2() {
    testFloatStats(StatsHelper.V2);
  }

  private void testFloatStats(StatsHelper helper) {
    // make fake stats and verify the size check
    FloatStatistics stats = new FloatStatistics();
    stats.incrementNumNulls(3004);
    float min = Float.MIN_VALUE;
    float max = Float.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertThat(Float.intBitsToFloat(BytesUtils.bytesToInt(formatStats.getMin())))
        .as("Min should match")
        .isCloseTo(min, offset(0.000001f));
    assertThat(Float.intBitsToFloat(BytesUtils.bytesToInt(formatStats.getMax())))
        .as("Max should match")
        .isCloseTo(max, offset(0.000001f));
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);
  }

  @Test
  public void testDoubleStatsV1() {
    testDoubleStats(StatsHelper.V1);
  }

  @Test
  public void testDoubleStatsV2() {
    testDoubleStats(StatsHelper.V2);
  }

  private void testDoubleStats(StatsHelper helper) {
    // make fake stats and verify the size check
    DoubleStatistics stats = new DoubleStatistics();
    stats.incrementNumNulls(3004);
    double min = Double.MIN_VALUE;
    double max = Double.MAX_VALUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertThat(Double.longBitsToDouble(BytesUtils.bytesToLong(formatStats.getMin())))
        .as("Min should match")
        .isCloseTo(min, offset(0.000001));
    assertThat(Double.longBitsToDouble(BytesUtils.bytesToLong(formatStats.getMax())))
        .as("Max should match")
        .isCloseTo(max, offset(0.000001));
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);
  }

  @Test
  public void testBooleanStatsV1() {
    testBooleanStats(StatsHelper.V1);
  }

  @Test
  public void testBooleanStatsV2() {
    testBooleanStats(StatsHelper.V2);
  }

  private void testBooleanStats(StatsHelper helper) {
    // make fake stats and verify the size check
    BooleanStatistics stats = new BooleanStatistics();
    stats.incrementNumNulls(3004);
    boolean min = Boolean.FALSE;
    boolean max = Boolean.TRUE;
    stats.updateStats(min);
    stats.updateStats(max);

    org.apache.parquet.format.Statistics formatStats = helper.toParquetStatistics(stats);

    assertThat(BytesUtils.bytesToBool(formatStats.getMin()))
        .as("Min should match")
        .isEqualTo(min);
    assertThat(BytesUtils.bytesToBool(formatStats.getMax()))
        .as("Max should match")
        .isEqualTo(max);
    assertThat(formatStats.getNull_count()).as("Num nulls should match").isEqualTo(3004);
  }

  @Test
  public void testIgnoreStatsWithSignedSortOrder() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("z"));
    stats.incrementNumNulls();

    PrimitiveType binaryType =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("b");
    Statistics convertedStats = converter.fromParquetStatistics(
        Version.FULL_VERSION, StatsHelper.V1.toParquetStatistics(stats), binaryType);

    assertThat(convertedStats.hasNonNullValue())
        .as("Stats should not include min/max: " + convertedStats)
        .isFalse();
    assertThat(convertedStats.isNumNullsSet())
        .as("Stats should have null count: " + convertedStats)
        .isTrue();
    assertThat(convertedStats.getNumNulls())
        .as("Stats should have 3 nulls: " + convertedStats)
        .isEqualTo(3L);
  }

  @Test
  public void testStillUseStatsWithSignedSortOrderIfSingleValueV1() {
    testStillUseStatsWithSignedSortOrderIfSingleValue(StatsHelper.V1);
  }

  @Test
  public void testStillUseStatsWithSignedSortOrderIfSingleValueV2() {
    testStillUseStatsWithSignedSortOrderIfSingleValue(StatsHelper.V2);
  }

  private void testStillUseStatsWithSignedSortOrderIfSingleValue(StatsHelper helper) {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();

    PrimitiveType binaryType =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("b");
    Statistics convertedStats = converter.fromParquetStatistics(
        Version.FULL_VERSION, ParquetMetadataConverter.toParquetStatistics(stats), binaryType);

    assertThat(convertedStats.isEmpty())
        .as("Stats should not be empty: " + convertedStats)
        .isFalse();
    assertThat(convertedStats.getMinBytes())
        .as("min == max: " + convertedStats)
        .isEqualTo(convertedStats.getMaxBytes());
  }

  @Test
  public void testUseStatsWithSignedSortOrderV1() {
    testUseStatsWithSignedSortOrder(StatsHelper.V1);
  }

  @Test
  public void testUseStatsWithSignedSortOrderV2() {
    testUseStatsWithSignedSortOrder(StatsHelper.V2);
  }

  private void testUseStatsWithSignedSortOrder(StatsHelper helper) {
    // override defaults and use stats that were accumulated using signed order
    Configuration conf = new Configuration();
    conf.setBoolean("parquet.strings.signed-min-max.enabled", true);

    ParquetMetadataConverter converter = new ParquetMetadataConverter(conf);
    BinaryStatistics stats = new BinaryStatistics();
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("A"));
    stats.incrementNumNulls();
    stats.updateStats(Binary.fromString("z"));
    stats.incrementNumNulls();

    PrimitiveType binaryType =
        Types.required(PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("b");
    Statistics convertedStats =
        converter.fromParquetStatistics(Version.FULL_VERSION, helper.toParquetStatistics(stats), binaryType);

    assertThat(convertedStats.isEmpty()).as("Stats should not be empty").isFalse();
    assertThat(convertedStats.isNumNullsSet()).isTrue();
    assertThat(convertedStats.getNumNulls()).as("Should have 3 nulls").isEqualTo(3);
    if (helper == StatsHelper.V1) {
      assertThat(convertedStats.hasNonNullValue())
          .as("Min-max should be null for V1 stats")
          .isFalse();
    } else {
      assertThat(convertedStats.genericGetMin())
          .as("Should have correct min (unsigned sort)")
          .isEqualTo(Binary.fromString("A"));
      assertThat(convertedStats.genericGetMax())
          .as("Should have correct max (unsigned sort)")
          .isEqualTo(Binary.fromString("z"));
    }
  }

  @Test
  public void testFloat16Stats() {
    Statistics stats = Statistics.createStats(
        new PrimitiveType(Repetition.OPTIONAL, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, 2, "float16")
            .withLogicalTypeAnnotation(LogicalTypeAnnotation.float16Type()));
    stats.updateStats(toBinary(0xff, 0x03));
    stats.updateStats(toBinary(0xff, 0x7b));
    String expectedMinStr = "6.097555E-5";
    String expectedMaxStr = "65504.0";
    assertThat(stats.minAsString()).isEqualTo(expectedMinStr);
    assertThat(stats.maxAsString()).isEqualTo(expectedMaxStr);
  }

  private Binary toBinary(int... bytes) {
    byte[] array = new byte[bytes.length];
    for (int i = 0; i < array.length; ++i) {
      array[i] = (byte) bytes[i];
    }
    return Binary.fromConstantByteArray(array);
  }

  @Test
  public void testMissingValuesFromStats() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    PrimitiveType type = Types.required(PrimitiveTypeName.INT32).named("test_int32");

    org.apache.parquet.format.Statistics formatStats = new org.apache.parquet.format.Statistics();
    Statistics<?> stats = converter.fromParquetStatistics(Version.FULL_VERSION, formatStats, type);
    assertThat(stats.isNumNullsSet()).isFalse();
    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.isEmpty()).isTrue();
    assertThat(stats.getNumNulls()).isEqualTo(-1);

    formatStats.clear();
    formatStats.setMin(BytesUtils.intToBytes(-100));
    formatStats.setMax(BytesUtils.intToBytes(100));
    stats = converter.fromParquetStatistics(Version.FULL_VERSION, formatStats, type);
    assertThat(stats.isNumNullsSet()).isFalse();
    assertThat(stats.hasNonNullValue()).isTrue();
    assertThat(stats.isEmpty()).isFalse();
    assertThat(stats.getNumNulls()).isEqualTo(-1);
    assertThat(stats.genericGetMin()).isEqualTo(-100);
    assertThat(stats.genericGetMax()).isEqualTo(100);

    formatStats.clear();
    formatStats.setNull_count(2000);
    stats = converter.fromParquetStatistics(Version.FULL_VERSION, formatStats, type);
    assertThat(stats.isNumNullsSet()).isTrue();
    assertThat(stats.hasNonNullValue()).isFalse();
    assertThat(stats.isEmpty()).isFalse();
    assertThat(stats.getNumNulls()).isEqualTo(2000);
  }

  @Test
  public void testSkippedV2Stats() {
    testSkippedV2Stats(
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(12)
            .as(OriginalType.INTERVAL)
            .named(""),
        new BigInteger("12345678"),
        new BigInteger("12345679"));
    testSkippedV2Stats(
        Types.optional(PrimitiveTypeName.INT96).named(""),
        new BigInteger("-75687987"),
        new BigInteger("45367657"));
  }

  private void testSkippedV2Stats(PrimitiveType type, Object min, Object max) {
    Statistics<?> stats = createStats(type, min, max);
    org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(statistics.isSetMin()).isFalse();
    assertThat(statistics.isSetMax()).isFalse();
    assertThat(statistics.isSetMin_value()).isFalse();
    assertThat(statistics.isSetMax_value()).isFalse();
  }

  @Test
  public void testV2OnlyStats() {
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_8).named(""), 0x7F, 0x80);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_16).named(""), 0x7FFF, 0x8000);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_32).named(""), 0x7FFFFFFF, 0x80000000);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.INT64).as(OriginalType.UINT_64).named(""),
        0x7FFFFFFFFFFFFFFFL,
        0x8000000000000000L);
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.BINARY)
            .as(OriginalType.DECIMAL)
            .precision(6)
            .named(""),
        new BigInteger("-765875"),
        new BigInteger("876856"));
    testV2OnlyStats(
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(14)
            .as(OriginalType.DECIMAL)
            .precision(7)
            .named(""),
        new BigInteger("-6769643"),
        new BigInteger("9864675"));
  }

  private void testV2OnlyStats(PrimitiveType type, Object min, Object max) {
    Statistics<?> stats = createStats(type, min, max);
    org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(statistics.isSetMin()).isFalse();
    assertThat(statistics.isSetMax()).isFalse();
    assertThat(statistics.min_value).isEqualTo(ByteBuffer.wrap(stats.getMinBytes()));
    assertThat(statistics.max_value).isEqualTo(ByteBuffer.wrap(stats.getMaxBytes()));
  }

  @Test
  public void testV2StatsEqualMinMax() {
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_8).named(""), 93, 93);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_16).named(""), -5892, -5892);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT32).as(OriginalType.UINT_32).named(""), 234998934, 234998934);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT64).as(OriginalType.UINT_64).named(""),
        -2389943895984985L,
        -2389943895984985L);
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.BINARY)
            .as(OriginalType.DECIMAL)
            .precision(6)
            .named(""),
        new BigInteger("823749"),
        new BigInteger("823749"));
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
            .length(14)
            .as(OriginalType.DECIMAL)
            .precision(7)
            .named(""),
        new BigInteger("-8752832"),
        new BigInteger("-8752832"));
    testV2StatsEqualMinMax(
        Types.optional(PrimitiveTypeName.INT96).named(""),
        new BigInteger("81032984"),
        new BigInteger("81032984"));
  }

  private void testV2StatsEqualMinMax(PrimitiveType type, Object min, Object max) {
    Statistics<?> stats = createStats(type, min, max);
    org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(statistics.min).isEqualTo(ByteBuffer.wrap(stats.getMinBytes()));
    assertThat(statistics.max).isEqualTo(ByteBuffer.wrap(stats.getMaxBytes()));
    assertThat(statistics.min_value).isEqualTo(ByteBuffer.wrap(stats.getMinBytes()));
    assertThat(statistics.max_value).isEqualTo(ByteBuffer.wrap(stats.getMaxBytes()));
  }

  private static <T> Statistics<?> createStats(PrimitiveType type, T min, T max) {
    Class<?> c = min.getClass();
    if (c == Integer.class) {
      return createStatsTyped(type, (Integer) min, (Integer) max);
    } else if (c == Long.class) {
      return createStatsTyped(type, (Long) min, (Long) max);
    } else if (c == BigInteger.class) {
      return createStatsTyped(type, (BigInteger) min, (BigInteger) max);
    }
    fail("Not implemented");
    return null;
  }

  private static Statistics<?> createStatsTyped(PrimitiveType type, int min, int max) {
    Statistics<?> stats = Statistics.createStats(type);
    stats.updateStats(max);
    stats.updateStats(min);
    assertThat(stats.genericGetMin()).isEqualTo(min);
    assertThat(stats.genericGetMax()).isEqualTo(max);
    return stats;
  }

  private static Statistics<?> createStatsTyped(PrimitiveType type, long min, long max) {
    Statistics<?> stats = Statistics.createStats(type);
    stats.updateStats(max);
    stats.updateStats(min);
    assertThat(stats.genericGetMin()).isEqualTo(min);
    assertThat(stats.genericGetMax()).isEqualTo(max);
    return stats;
  }

  private static Statistics<?> createStatsTyped(PrimitiveType type, BigInteger min, BigInteger max) {
    Statistics<?> stats = Statistics.createStats(type);
    Binary minBinary = FixedBinaryTestUtils.getFixedBinary(type, min);
    Binary maxBinary = FixedBinaryTestUtils.getFixedBinary(type, max);
    stats.updateStats(maxBinary);
    stats.updateStats(minBinary);
    assertThat(stats.genericGetMin()).isEqualTo(minBinary);
    assertThat(stats.genericGetMax()).isEqualTo(maxBinary);
    return stats;
  }

  private static ParquetMetadata createParquetMetaData(Encoding dicEncoding, Encoding dataEncoding) {
    return createParquetMetaData(dicEncoding, dataEncoding, true);
  }

  private static ParquetMetadata createParquetMetaData(
      Encoding dicEncoding, Encoding dataEncoding, boolean includeEncodingStats) {
    MessageType schema = parseMessageType("message schema { optional int32 col (INT_32); }");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<String, String>(), null);
    List<BlockMetaData> blockMetaDataList = new ArrayList<BlockMetaData>();
    BlockMetaData blockMetaData = new BlockMetaData();
    EncodingStats es = null;
    if (includeEncodingStats) {
      EncodingStats.Builder builder = new EncodingStats.Builder();
      if (dicEncoding != null) {
        builder.addDictEncoding(dicEncoding).build();
      }
      builder.addDataEncoding(dataEncoding);
      es = builder.build();
    }
    Set<org.apache.parquet.column.Encoding> e = new HashSet<org.apache.parquet.column.Encoding>();
    if (!includeEncodingStats) {
      if (dicEncoding != null) {
        e.add(dicEncoding);
      }
      e.add(dataEncoding);
    }
    PrimitiveTypeName t = PrimitiveTypeName.INT32;
    ColumnPath p = ColumnPath.get("col");
    CompressionCodecName c = CompressionCodecName.UNCOMPRESSED;
    BinaryStatistics s = new BinaryStatistics();
    ColumnChunkMetaData md = ColumnChunkMetaData.get(p, t, c, es, e, s, 20, 30, 0, 0, 0);
    blockMetaData.addColumn(md);
    blockMetaDataList.add(blockMetaData);
    return new ParquetMetadata(fileMetaData, blockMetaDataList);
  }

  private enum StatsHelper {
    // Only min and max are filled (min_value and max_value are not)
    V1() {
      @Override
      public org.apache.parquet.format.Statistics toParquetStatistics(Statistics<?> stats) {
        org.apache.parquet.format.Statistics statistics = ParquetMetadataConverter.toParquetStatistics(stats);
        statistics.unsetMin_value();
        statistics.unsetMax_value();
        return statistics;
      }
    },
    // min_value and max_value are filled (min and max might be filled as well)
    V2() {
      @Override
      public org.apache.parquet.format.Statistics toParquetStatistics(Statistics<?> stats) {
        return ParquetMetadataConverter.toParquetStatistics(stats);
      }
    };

    public abstract org.apache.parquet.format.Statistics toParquetStatistics(Statistics<?> stats);
  }

  @Test
  public void testColumnOrders() throws IOException {
    MessageType schema = parseMessageType("message test {"
        + "  optional binary binary_col;" // Normal column with type defined column order -> typeDefined
        + "  optional group map_col (MAP) {"
        + "    repeated group map (MAP_KEY_VALUE) {"
        + "        required binary key (UTF8);" // Key to be hacked to have unknown column order -> undefined
        + "        optional group list_col (LIST) {"
        + "          repeated group list {"
        + "            optional int96 array_element;" // INT96 element with type defined column order ->
        // undefined
        + "          }"
        + "        }"
        + "    }"
        + "  }"
        + "}");
    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<String, String>(), null);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ArrayList<BlockMetaData>());
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData formatMetadata = converter.toParquetMetadata(1, metadata);

    List<org.apache.parquet.format.ColumnOrder> columnOrders = formatMetadata.getColumn_orders();
    assertThat(columnOrders).hasSize(3);
    for (org.apache.parquet.format.ColumnOrder columnOrder : columnOrders) {
      assertThat(columnOrder.isSetTYPE_ORDER()).isTrue();
    }

    // Simulate that thrift got a union type that is not in the generated code
    // (when the file contains a not-yet-supported column order)
    columnOrders.get(1).clear();

    MessageType resultSchema =
        converter.fromParquetMetadata(formatMetadata).getFileMetaData().getSchema();
    List<ColumnDescriptor> columns = resultSchema.getColumns();
    assertThat(columns).hasSize(3);
    assertThat(columns.get(0).getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.typeDefined());
    assertThat(columns.get(1).getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.undefined());
    assertThat(columns.get(2).getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.undefined());
  }

  @Test
  public void testOffsetIndexConversion() {
    for (boolean withSizeStats : new boolean[] {false, true}) {
      OffsetIndexBuilder builder = OffsetIndexBuilder.getBuilder();
      builder.add(1000, 10000, 0, withSizeStats ? Optional.of(11L) : Optional.empty());
      builder.add(22000, 12000, 100, withSizeStats ? Optional.of(22L) : Optional.empty());
      OffsetIndex offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(
          ParquetMetadataConverter.toParquetOffsetIndex(builder.build(100000)));
      assertThat(offsetIndex.getPageCount()).isEqualTo(2);
      assertThat(offsetIndex.getOffset(0)).isEqualTo(101000);
      assertThat(offsetIndex.getCompressedPageSize(0)).isEqualTo(10000);
      assertThat(offsetIndex.getFirstRowIndex(0)).isEqualTo(0);
      assertThat(offsetIndex.getOffset(1)).isEqualTo(122000);
      assertThat(offsetIndex.getCompressedPageSize(1)).isEqualTo(12000);
      assertThat(offsetIndex.getFirstRowIndex(1)).isEqualTo(100);
      if (withSizeStats) {
        assertThat(offsetIndex.getUnencodedByteArrayDataBytes(0)).isEqualTo(Optional.of(11L));
        assertThat(offsetIndex.getUnencodedByteArrayDataBytes(1)).isEqualTo(Optional.of(22L));
      } else {
        assertThat(offsetIndex.getUnencodedByteArrayDataBytes(0)).isEmpty();
        assertThat(offsetIndex.getUnencodedByteArrayDataBytes(1)).isEmpty();
      }
    }
  }

  @Test
  public void testColumnIndexConversion() {
    for (boolean withSizeStats : new boolean[] {false, true}) {
      PrimitiveType type = Types.required(PrimitiveTypeName.INT64).named("test_int64");
      ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);
      Statistics<?> stats = Statistics.createStats(type);
      stats.incrementNumNulls(16);
      stats.updateStats(-100l);
      stats.updateStats(100l);
      builder.add(
          stats,
          withSizeStats ? new SizeStatistics(type, 0, LongArrayList.of(1, 2), LongArrayList.of(6, 5)) : null);
      stats = Statistics.createStats(type);
      stats.incrementNumNulls(111);
      builder.add(
          stats,
          withSizeStats ? new SizeStatistics(type, 0, LongArrayList.of(3, 4), LongArrayList.of(4, 3)) : null);
      stats = Statistics.createStats(type);
      stats.updateStats(200l);
      stats.updateStats(500l);
      builder.add(
          stats,
          withSizeStats ? new SizeStatistics(type, 0, LongArrayList.of(5, 6), LongArrayList.of(2, 1)) : null);
      org.apache.parquet.format.ColumnIndex parquetColumnIndex =
          ParquetMetadataConverter.toParquetColumnIndex(type, builder.build());
      ColumnIndex columnIndex = ParquetMetadataConverter.fromParquetColumnIndex(type, parquetColumnIndex);
      assertThat(columnIndex.getBoundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
      assertThat(columnIndex.getNullPages()).containsExactly(false, true, false);
      assertThat(columnIndex.getNullCounts()).containsExactly(16L, 111L, 0L);
      assertThat(columnIndex.getMinValues())
          .containsExactly(
              ByteBuffer.wrap(BytesUtils.longToBytes(-100L)),
              ByteBuffer.allocate(0),
              ByteBuffer.wrap(BytesUtils.longToBytes(200L)));
      assertThat(columnIndex.getMaxValues())
          .containsExactly(
              ByteBuffer.wrap(BytesUtils.longToBytes(100L)),
              ByteBuffer.allocate(0),
              ByteBuffer.wrap(BytesUtils.longToBytes(500L)));

      assertThat(ParquetMetadataConverter.toParquetColumnIndex(
              Types.required(PrimitiveTypeName.INT32).named("test_int32"), null))
          .as("Should handle null column index")
          .isNull();
      assertThat(ParquetMetadataConverter.toParquetColumnIndex(
              Types.required(PrimitiveTypeName.INT96).named("test_int96"), columnIndex))
          .as("Should ignore unsupported types")
          .isNull();
      assertThat(ParquetMetadataConverter.fromParquetColumnIndex(
              Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                  .length(12)
                  .as(OriginalType.INTERVAL)
                  .named("test_interval"),
              parquetColumnIndex))
          .as("Should ignore unsupported types")
          .isNull();

      if (withSizeStats) {
        assertThat(columnIndex.getRepetitionLevelHistogram()).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(columnIndex.getDefinitionLevelHistogram()).containsExactly(6L, 5L, 4L, 3L, 2L, 1L);
      } else {
        assertThat(columnIndex.getRepetitionLevelHistogram()).isEmpty();
        assertThat(columnIndex.getDefinitionLevelHistogram()).isEmpty();
      }
    }
  }

  @Test
  public void testMapLogicalType() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType expected = Types.buildMessage()
        .requiredGroup()
        .as(mapType())
        .repeatedGroup()
        .as(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
        .required(PrimitiveTypeName.BINARY)
        .as(stringType())
        .named("key")
        .required(PrimitiveTypeName.INT32)
        .named("value")
        .named("key_value")
        .named("testMap")
        .named("Message");

    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    assertThat(parquetSchema).hasSize(5);
    assertThat(parquetSchema.get(0)).isEqualTo(new SchemaElement("Message").setNum_children(1));
    assertThat(parquetSchema.get(1))
        .isEqualTo(new SchemaElement("testMap")
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setNum_children(1)
            .setConverted_type(ConvertedType.MAP)
            .setLogicalType(LogicalType.MAP(new MapType())));
    // PARQUET-1879 ensure that LogicalType is not written (null) but ConvertedType is MAP_KEY_VALUE for
    // backwards-compatibility
    assertThat(parquetSchema.get(2))
        .isEqualTo(new SchemaElement("key_value")
            .setRepetition_type(FieldRepetitionType.REPEATED)
            .setNum_children(2)
            .setConverted_type(ConvertedType.MAP_KEY_VALUE)
            .setLogicalType(null));
    assertThat(parquetSchema.get(3))
        .isEqualTo(new SchemaElement("key")
            .setType(Type.BYTE_ARRAY)
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setConverted_type(ConvertedType.UTF8)
            .setLogicalType(LogicalType.STRING(new StringType())));
    assertThat(parquetSchema.get(4))
        .isEqualTo(new SchemaElement("value")
            .setType(Type.INT32)
            .setRepetition_type(FieldRepetitionType.REQUIRED)
            .setConverted_type(null)
            .setLogicalType(null));

    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertThat(schema).isEqualTo(expected);
  }

  @Test
  public void testMapLogicalTypeReadWrite() throws Exception {
    MessageType messageType = Types.buildMessage()
        .requiredGroup()
        .as(mapType())
        .repeatedGroup()
        .as(LogicalTypeAnnotation.MapKeyValueTypeAnnotation.getInstance())
        .required(PrimitiveTypeName.BINARY)
        .as(stringType())
        .named("key")
        .required(PrimitiveTypeName.INT64)
        .named("value")
        .named("key_value")
        .named("testMap")
        .named("example");

    verifyMapMessageType(messageType, "key_value");
  }

  @Test
  public void testMapConvertedTypeReadWrite() throws Exception {
    List<SchemaElement> oldConvertedTypeSchemaElements = new ArrayList<>();
    oldConvertedTypeSchemaElements.add(new SchemaElement("example").setNum_children(1));
    oldConvertedTypeSchemaElements.add(new SchemaElement("testMap")
        .setRepetition_type(FieldRepetitionType.REQUIRED)
        .setNum_children(1)
        .setConverted_type(ConvertedType.MAP)
        .setLogicalType(null));
    oldConvertedTypeSchemaElements.add(new SchemaElement("map")
        .setRepetition_type(FieldRepetitionType.REPEATED)
        .setNum_children(2)
        .setConverted_type(ConvertedType.MAP_KEY_VALUE)
        .setLogicalType(null));
    oldConvertedTypeSchemaElements.add(new SchemaElement("key")
        .setType(Type.BYTE_ARRAY)
        .setRepetition_type(FieldRepetitionType.REQUIRED)
        .setConverted_type(ConvertedType.UTF8)
        .setLogicalType(null));
    oldConvertedTypeSchemaElements.add(new SchemaElement("value")
        .setType(Type.INT64)
        .setRepetition_type(FieldRepetitionType.REQUIRED)
        .setConverted_type(null)
        .setLogicalType(null));

    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    MessageType messageType = parquetMetadataConverter.fromParquetSchema(oldConvertedTypeSchemaElements, null);

    verifyMapMessageType(messageType, "map");
  }

  @Test
  public void testVariantLogicalType() {
    byte specVersion = 1;
    MessageType expected = Types.buildMessage()
        .requiredGroup()
        .as(variantType(specVersion))
        .required(PrimitiveTypeName.BINARY)
        .named("metadata")
        .required(PrimitiveTypeName.BINARY)
        .named("value")
        .named("v")
        .named("example");

    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(expected);
    MessageType schema = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);
    assertThat(schema).isEqualTo(expected);
    LogicalTypeAnnotation logicalType = schema.getType("v").getLogicalTypeAnnotation();
    assertThat(logicalType).isEqualTo(LogicalTypeAnnotation.variantType(specVersion));
    assertThat(((LogicalTypeAnnotation.VariantLogicalTypeAnnotation) logicalType).getSpecVersion())
        .isEqualTo(specVersion);
  }

  private void verifyMapMessageType(final MessageType messageType, final String keyValueName) throws IOException {
    Path file = new Path(temporaryFolder.newFolder("verifyMapMessageType").getPath(), keyValueName + ".parquet");

    try (ParquetWriter<Group> writer =
        ExampleParquetWriter.builder(file).withType(messageType).build()) {
      final Group group = new SimpleGroup(messageType);
      final Group mapGroup = group.addGroup("testMap");

      for (int index = 0; index < 5; index++) {
        final Group keyValueGroup = mapGroup.addGroup(keyValueName);
        keyValueGroup.add("key", Binary.fromString("key" + index));
        keyValueGroup.add("value", 100L + index);
      }

      writer.write(group);
    }

    try (ParquetReader<Group> reader =
        ParquetReader.builder(new GroupReadSupport(), file).build()) {
      Group group = reader.read();

      assertThat(group).isNotNull();

      Group testMap = group.getGroup("testMap", 0);
      assertThat(testMap).isNotNull();
      assertThat(testMap.getFieldRepetitionCount(keyValueName)).isEqualTo(5);

      for (int index = 0; index < 5; index++) {
        assertThat(testMap.getGroup(keyValueName, index).getString("key", 0))
            .isEqualTo("key" + index);
        assertThat(testMap.getGroup(keyValueName, index).getLong("value", 0))
            .isEqualTo(100L + index);
      }
    }
  }

  @Test
  public void testSizeStatisticsConversion() {
    PrimitiveType type = Types.required(PrimitiveTypeName.BINARY).named("test");
    List<Long> repLevelHistogram = List.of(1L, 2L, 3L, 4L, 5L);
    List<Long> defLevelHistogram = List.of(6L, 7L, 8L, 9L, 10L);
    SizeStatistics sizeStatistics = ParquetMetadataConverter.fromParquetSizeStatistics(
        ParquetMetadataConverter.toParquetSizeStatistics(
            new SizeStatistics(type, 1024, repLevelHistogram, defLevelHistogram)),
        type);
    assertThat(sizeStatistics.getType()).isEqualTo(type);
    assertThat(sizeStatistics.getUnencodedByteArrayDataBytes()).isEqualTo(Optional.of(1024L));
    assertThat(sizeStatistics.getRepetitionLevelHistogram()).containsExactlyElementsOf(repLevelHistogram);
    assertThat(sizeStatistics.getDefinitionLevelHistogram()).containsExactlyElementsOf(defLevelHistogram);
  }

  @Test
  public void testGeometryLogicalType() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    // Create schema with geometry type
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geometryType("EPSG:4326"))
        .named("geomField")
        .named("Message");

    // Convert to parquet schema and back
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);

    // Verify the logical type is preserved
    assertThat(actual).isEqualTo(schema);

    PrimitiveType primitiveType = actual.getType("geomField").asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    assertThat(logicalType).isInstanceOf(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation.class);
    assertThat(((LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) logicalType).getCrs())
        .isEqualTo("EPSG:4326");
  }

  @Test
  public void testGeographyLogicalType() {
    ParquetMetadataConverter parquetMetadataConverter = new ParquetMetadataConverter();

    // Create schema with geography type
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.BINARY)
        .as(LogicalTypeAnnotation.geographyType("EPSG:4326", EdgeInterpolationAlgorithm.SPHERICAL))
        .named("geogField")
        .named("Message");

    // Convert to parquet schema and back
    List<SchemaElement> parquetSchema = parquetMetadataConverter.toParquetSchema(schema);
    MessageType actual = parquetMetadataConverter.fromParquetSchema(parquetSchema, null);

    // Verify the logical type is preserved
    assertThat(actual).isEqualTo(schema);

    PrimitiveType primitiveType = actual.getType("geogField").asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
    assertThat(logicalType).isInstanceOf(LogicalTypeAnnotation.GeographyLogicalTypeAnnotation.class);

    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyType =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) logicalType;
    assertThat(geographyType.getCrs()).isEqualTo("EPSG:4326");
    assertThat(geographyType.getAlgorithm()).isEqualTo(EdgeInterpolationAlgorithm.SPHERICAL);
  }

  @Test
  public void testGeometryLogicalTypeWithMissingCrs() {
    // Create a Geometry logical type without specifying CRS
    GeometryType geometryType = new GeometryType();
    LogicalType logicalType = new LogicalType();
    logicalType.setGEOMETRY(geometryType);

    // Convert to LogicalTypeAnnotation
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertThat(annotation).as("Geometry annotation should not be null").isNotNull();
    assertThat(annotation)
        .as("Should be a GeometryLogicalTypeAnnotation")
        .isInstanceOf(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation.class);

    LogicalTypeAnnotation.GeometryLogicalTypeAnnotation geometryAnnotation =
        (LogicalTypeAnnotation.GeometryLogicalTypeAnnotation) annotation;

    // Default behavior should use null or empty CRS
    assertThat(geometryAnnotation.getCrs())
        .as("CRS should be null or empty when not specified")
        .isNull();
  }

  @Test
  public void testGeographyLogicalTypeWithMissingParameters() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create a Geography logical type without CRS and algorithm
    GeographyType geographyType = new GeographyType();
    LogicalType logicalType = new LogicalType();
    logicalType.setGEOGRAPHY(geographyType);

    // Convert to LogicalTypeAnnotation
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertThat(annotation).as("Geography annotation should not be null").isNotNull();
    assertThat(annotation)
        .as("Should be a GeographyLogicalTypeAnnotation")
        .isInstanceOf(LogicalTypeAnnotation.GeographyLogicalTypeAnnotation.class);

    // Check that optional parameters are handled correctly
    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyAnnotation =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) annotation;
    assertThat(geographyAnnotation.getCrs())
        .as("CRS should be null when not specified")
        .isNull();
    // Most implementations default to LINEAR when algorithm is not specified
    assertThat(geographyAnnotation.getAlgorithm())
        .as("Algorithm should be null when not specified")
        .isNull();

    // Now test the round-trip conversion
    LogicalType roundTripType = converter.convertToLogicalType(annotation);
    assertThat(roundTripType.getSetField())
        .as("setField should be GEOGRAPHY")
        .isEqualTo(LogicalType._Fields.GEOGRAPHY);
    assertThat(roundTripType.getGEOGRAPHY().getCrs())
        .as("Round trip CRS should still be null")
        .isNull();
    assertThat(roundTripType.getGEOGRAPHY().getAlgorithm())
        .as("Round trip Algorithm should be null")
        .isNull();
  }

  @Test
  public void testGeographyLogicalTypeWithAlgorithmButNoCrs() {
    // Create a Geography logical type with algorithm but no CRS
    GeographyType geographyType = new GeographyType();
    geographyType.setAlgorithm(org.apache.parquet.format.EdgeInterpolationAlgorithm.SPHERICAL);
    LogicalType logicalType = new LogicalType();
    logicalType.setGEOGRAPHY(geographyType);

    // Convert to LogicalTypeAnnotation
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    LogicalTypeAnnotation annotation = converter.getLogicalTypeAnnotation(logicalType);

    // Verify the annotation is created correctly
    assertThat(annotation).as("Geography annotation should not be null").isNotNull();
    LogicalTypeAnnotation.GeographyLogicalTypeAnnotation geographyAnnotation =
        (LogicalTypeAnnotation.GeographyLogicalTypeAnnotation) annotation;

    // CRS should be null/empty but algorithm should be set
    assertThat(geographyAnnotation.getCrs())
        .as("CRS should be null or empty")
        .isNull();
    assertThat(geographyAnnotation.getAlgorithm())
        .as("Algorithm should be SPHERICAL")
        .isEqualTo(EdgeInterpolationAlgorithm.SPHERICAL);
  }

  @Test
  public void testGeospatialStatisticsConversion() {
    // Create a ParquetMetadataConverter
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create a valid BoundingBox with all fields set
    org.apache.parquet.column.statistics.geospatial.BoundingBox bbox =
        new org.apache.parquet.column.statistics.geospatial.BoundingBox(
            1.0, 2.0, // xmin, xmax
            3.0, 4.0, // ymin, ymax
            5.0, 6.0, // zmin, zmax
            7.0, 8.0 // mmin, mmax
            );

    // Create GeospatialTypes with some example type values
    Set<Integer> types = new HashSet<>(List.of(1, 2, 3));
    GeospatialTypes geospatialTypes = new GeospatialTypes(types);

    // Create GeospatialStatistics with the bbox and types
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics origStats =
        new org.apache.parquet.column.statistics.geospatial.GeospatialStatistics(bbox, geospatialTypes);

    // Convert to Thrift format
    GeospatialStatistics thriftStats = converter.toParquetGeospatialStatistics(origStats);

    // Verify conversion to Thrift
    assertThat(thriftStats)
        .as("Thrift GeospatialStatistics should not be null")
        .isNotNull();
    assertThat(thriftStats.isSetBbox()).as("BoundingBox should be set").isTrue();
    assertThat(thriftStats.isSetGeospatial_types())
        .as("Geospatial types should be set")
        .isTrue();

    // Check BoundingBox values
    BoundingBox thriftBbox = thriftStats.getBbox();
    assertThat(thriftBbox.getXmin()).isCloseTo(1.0, offset(0.0001));
    assertThat(thriftBbox.getXmax()).isCloseTo(2.0, offset(0.0001));
    assertThat(thriftBbox.getYmin()).isCloseTo(3.0, offset(0.0001));
    assertThat(thriftBbox.getYmax()).isCloseTo(4.0, offset(0.0001));
    assertThat(thriftBbox.getZmin()).isCloseTo(5.0, offset(0.0001));
    assertThat(thriftBbox.getZmax()).isCloseTo(6.0, offset(0.0001));
    assertThat(thriftBbox.getMmin()).isCloseTo(7.0, offset(0.0001));
    assertThat(thriftBbox.getMmax()).isCloseTo(8.0, offset(0.0001));

    // Check geospatial types
    List<Integer> thriftTypes = thriftStats.getGeospatial_types();
    assertThat(thriftTypes).hasSize(3);
    assertThat(thriftTypes).contains(1, 2, 3);

    // Create primitive geometry type for conversion back
    LogicalTypeAnnotation geometryAnnotation = LogicalTypeAnnotation.geometryType("EPSG:4326");
    PrimitiveType geometryType =
        Types.required(PrimitiveTypeName.BINARY).as(geometryAnnotation).named("geometry");

    // Convert back from Thrift format
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics convertedStats =
        ParquetMetadataConverter.fromParquetStatistics(thriftStats, geometryType);

    // Verify conversion from Thrift
    assertThat(convertedStats)
        .as("Converted GeospatialStatistics should not be null")
        .isNotNull();
    assertThat(convertedStats.getBoundingBox())
        .as("BoundingBox should not be null")
        .isNotNull();
    assertThat(convertedStats.getGeospatialTypes())
        .as("GeospatialTypes should not be null")
        .isNotNull();

    // Check BoundingBox values
    org.apache.parquet.column.statistics.geospatial.BoundingBox convertedBbox = convertedStats.getBoundingBox();
    assertThat(convertedBbox.getXMin()).isCloseTo(1.0, offset(0.0001));
    assertThat(convertedBbox.getXMax()).isCloseTo(2.0, offset(0.0001));
    assertThat(convertedBbox.getYMin()).isCloseTo(3.0, offset(0.0001));
    assertThat(convertedBbox.getYMax()).isCloseTo(4.0, offset(0.0001));
    assertThat(convertedBbox.getZMin()).isCloseTo(5.0, offset(0.0001));
    assertThat(convertedBbox.getZMax()).isCloseTo(6.0, offset(0.0001));
    assertThat(convertedBbox.getMMin()).isCloseTo(7.0, offset(0.0001));
    assertThat(convertedBbox.getMMax()).isCloseTo(8.0, offset(0.0001));

    // Check geospatial types
    Set<Integer> convertedTypes = convertedStats.getGeospatialTypes().getTypes();
    assertThat(convertedTypes).hasSize(3);
    assertThat(convertedTypes).contains(1, 2, 3);
  }

  @Test
  public void testGeospatialStatisticsWithNullBoundingBox() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create GeospatialStatistics with null bbox but valid types
    Set<Integer> types = new HashSet<>(List.of(1, 2, 3));
    GeospatialTypes geospatialTypes = new GeospatialTypes(types);
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics origStats =
        new org.apache.parquet.column.statistics.geospatial.GeospatialStatistics(null, geospatialTypes);

    // Convert to Thrift format
    GeospatialStatistics thriftStats = converter.toParquetGeospatialStatistics(origStats);

    // Verify conversion to Thrift
    assertThat(thriftStats)
        .as("Thrift GeospatialStatistics should not be null")
        .isNotNull();
    assertThat(thriftStats.isSetBbox()).as("BoundingBox should not be set").isFalse();
    assertThat(thriftStats.isSetGeospatial_types())
        .as("Geospatial types should be set")
        .isTrue();

    // Create primitive geometry type for conversion back
    LogicalTypeAnnotation geometryAnnotation = LogicalTypeAnnotation.geometryType("EPSG:4326");
    PrimitiveType geometryType =
        Types.required(PrimitiveTypeName.BINARY).as(geometryAnnotation).named("geometry");

    // Convert back from Thrift format
    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics convertedStats =
        ParquetMetadataConverter.fromParquetStatistics(thriftStats, geometryType);

    // Verify conversion from Thrift
    assertThat(convertedStats)
        .as("Converted GeospatialStatistics should not be null")
        .isNotNull();
    assertThat(convertedStats.getBoundingBox())
        .as("BoundingBox should be null")
        .isNull();
    assertThat(convertedStats.getGeospatialTypes())
        .as("GeospatialTypes should not be null")
        .isNotNull();
  }

  @Test
  public void testInvalidBoundingBox() {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();

    // Create an invalid BoundingBox with NaN values
    org.apache.parquet.column.statistics.geospatial.BoundingBox invalidBbox =
        new org.apache.parquet.column.statistics.geospatial.BoundingBox(
            Double.NaN,
            2.0, // xmin is NaN (invalid)
            3.0,
            4.0,
            5.0,
            6.0,
            7.0,
            8.0);

    org.apache.parquet.column.statistics.geospatial.GeospatialStatistics origStats =
        new org.apache.parquet.column.statistics.geospatial.GeospatialStatistics(invalidBbox, null);

    // Convert to Thrift format - should return null for invalid bbox
    GeospatialStatistics thriftStats = converter.toParquetGeospatialStatistics(origStats);
    assertThat(thriftStats).as("Should return null for invalid BoundingBox").isNull();
  }

  @Test
  public void testEdgeInterpolationAlgorithmConversion() {
    // Test conversion from Parquet to Thrift enum
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm parquetAlgo = EdgeInterpolationAlgorithm.SPHERICAL;
    org.apache.parquet.format.EdgeInterpolationAlgorithm thriftAlgo =
        ParquetMetadataConverter.fromParquetEdgeInterpolationAlgorithm(parquetAlgo);

    // convert the Thrift enum to the column schema enum
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm expected =
        org.apache.parquet.column.schema.EdgeInterpolationAlgorithm.SPHERICAL;
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm actual =
        ParquetMetadataConverter.toParquetEdgeInterpolationAlgorithm(thriftAlgo);
    assertThat(actual).isEqualTo(expected);

    // Test with null
    assertThat(ParquetMetadataConverter.fromParquetEdgeInterpolationAlgorithm(null))
        .isNull();
    assertThat(ParquetMetadataConverter.toParquetEdgeInterpolationAlgorithm(null))
        .isNull();
  }

  @Test
  public void testIEEE754TotalOrderColumnOrder() throws IOException {
    MessageType schema = Types.buildMessage()
        .required(PrimitiveTypeName.FLOAT)
        .columnOrder(ColumnOrder.ieee754TotalOrder())
        .named("float_ieee754")
        .required(PrimitiveTypeName.DOUBLE)
        .columnOrder(ColumnOrder.ieee754TotalOrder())
        .named("double_ieee754")
        .named("Message");

    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<String, String>(), null);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ArrayList<BlockMetaData>());
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData formatMetadata = converter.toParquetMetadata(1, metadata);

    List<org.apache.parquet.format.ColumnOrder> columnOrders = formatMetadata.getColumn_orders();
    assertThat(columnOrders).hasSize(2);
    for (org.apache.parquet.format.ColumnOrder columnOrder : columnOrders) {
      assertThat(columnOrder.isSetIEEE_754_TOTAL_ORDER()).isTrue();
    }

    MessageType resultSchema =
        converter.fromParquetMetadata(formatMetadata).getFileMetaData().getSchema();
    assertThat(resultSchema.getType("float_ieee754").asPrimitiveType().columnOrder())
        .isEqualTo(ColumnOrder.ieee754TotalOrder());
    assertThat(resultSchema.getType("double_ieee754").asPrimitiveType().columnOrder())
        .isEqualTo(ColumnOrder.ieee754TotalOrder());
  }

  @Test
  public void testNestedColumnOrdersUseLeafOrder() throws IOException {
    MessageType schema = Types.buildMessage()
        .requiredGroup()
        .required(PrimitiveTypeName.FLOAT)
        .columnOrder(ColumnOrder.ieee754TotalOrder())
        .named("a")
        .required(PrimitiveTypeName.DOUBLE)
        .named("b")
        .named("g")
        .required(PrimitiveTypeName.DOUBLE)
        .columnOrder(ColumnOrder.ieee754TotalOrder())
        .named("c")
        .named("Message");

    org.apache.parquet.hadoop.metadata.FileMetaData fileMetaData =
        new org.apache.parquet.hadoop.metadata.FileMetaData(schema, new HashMap<String, String>(), null);
    ParquetMetadata metadata = new ParquetMetadata(fileMetaData, new ArrayList<BlockMetaData>());
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    FileMetaData formatMetadata = converter.toParquetMetadata(1, metadata);

    MessageType resultSchema =
        converter.fromParquetMetadata(formatMetadata).getFileMetaData().getSchema();
    List<ColumnDescriptor> columns = resultSchema.getColumns();
    assertThat(columns).hasSize(3);
    assertThat(columns.get(0).getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.ieee754TotalOrder());
    assertThat(columns.get(1).getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.typeDefined());
    assertThat(columns.get(2).getPrimitiveType().columnOrder()).isEqualTo(ColumnOrder.ieee754TotalOrder());
  }

  @Test
  public void testStatisticsNanCountRoundTripFloat() {
    PrimitiveType type = Types.required(PrimitiveTypeName.FLOAT).named("test_float");
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(type);
    stats.updateStats(1.0f);
    stats.updateStats(Float.NaN);
    stats.updateStats(3.0f);
    stats.updateStats(Float.NaN);

    org.apache.parquet.format.Statistics formatStats = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(formatStats.isSetNan_count()).as("nan_count should be set").isTrue();
    assertThat(formatStats.getNan_count()).isEqualTo(2);

    Statistics<?> roundTrip = ParquetMetadataConverter.fromParquetStatisticsInternal(
        Version.FULL_VERSION, formatStats, type, ParquetMetadataConverter.SortOrder.SIGNED);
    assertThat(roundTrip.isNanCountSet()).isTrue();
    assertThat(roundTrip.getNanCount()).isEqualTo(2);
  }

  @Test
  public void testStatisticsNanCountRoundTripDouble() {
    PrimitiveType type = Types.required(PrimitiveTypeName.DOUBLE).named("test_double");
    DoubleStatistics stats = (DoubleStatistics) Statistics.createStats(type);
    stats.updateStats(1.0);
    stats.updateStats(Double.NaN);
    stats.updateStats(3.0);

    org.apache.parquet.format.Statistics formatStats = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(formatStats.isSetNan_count()).as("nan_count should be set").isTrue();
    assertThat(formatStats.getNan_count()).isEqualTo(1);

    Statistics<?> roundTrip = ParquetMetadataConverter.fromParquetStatisticsInternal(
        Version.FULL_VERSION, formatStats, type, ParquetMetadataConverter.SortOrder.SIGNED);
    assertThat(roundTrip.isNanCountSet()).isTrue();
    assertThat(roundTrip.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testStatisticsNanCountRoundTripFloat16() {
    PrimitiveType type = Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
        .length(2)
        .as(LogicalTypeAnnotation.float16Type())
        .named("test_float16");
    BinaryStatistics stats = (BinaryStatistics) Statistics.createStats(type);
    // FLOAT16 1.0 = 0x3C00
    stats.updateStats(Binary.fromConstantByteArray(new byte[] {0x00, 0x3C}));
    // FLOAT16 NaN = 0x7E00
    stats.updateStats(Binary.fromConstantByteArray(new byte[] {0x00, 0x7E}));

    org.apache.parquet.format.Statistics formatStats = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(formatStats.isSetNan_count()).as("nan_count should be set").isTrue();
    assertThat(formatStats.getNan_count()).isEqualTo(1);

    Statistics<?> roundTrip = ParquetMetadataConverter.fromParquetStatisticsInternal(
        Version.FULL_VERSION, formatStats, type, ParquetMetadataConverter.SortOrder.SIGNED);
    assertThat(roundTrip.isNanCountSet()).isTrue();
    assertThat(roundTrip.getNanCount()).isEqualTo(1);
  }

  @Test
  public void testStatisticsNanCountZeroRoundTrip() {
    PrimitiveType type = Types.required(PrimitiveTypeName.FLOAT).named("test_float");
    FloatStatistics stats = (FloatStatistics) Statistics.createStats(type);
    stats.updateStats(1.0f);
    stats.updateStats(2.0f);

    org.apache.parquet.format.Statistics formatStats = ParquetMetadataConverter.toParquetStatistics(stats);
    assertThat(formatStats.isSetNan_count())
        .as("nan_count should be set even when zero")
        .isTrue();
    assertThat(formatStats.getNan_count()).isEqualTo(0);

    Statistics<?> roundTrip = ParquetMetadataConverter.fromParquetStatisticsInternal(
        Version.FULL_VERSION, formatStats, type, ParquetMetadataConverter.SortOrder.SIGNED);
    assertThat(roundTrip.isNanCountSet()).isTrue();
    assertThat(roundTrip.getNanCount()).isEqualTo(0);
  }

  @Test
  public void testColumnIndexNanCountsRoundTrip() {
    PrimitiveType type = Types.required(PrimitiveTypeName.FLOAT)
        .columnOrder(ColumnOrder.ieee754TotalOrder())
        .named("test_float");
    ColumnIndexBuilder builder = ColumnIndexBuilder.getBuilder(type, Integer.MAX_VALUE);

    // Page 1: mixed NaN and non-NaN
    FloatStatistics stats1 = (FloatStatistics) Statistics.createStats(type);
    stats1.updateStats(1.0f);
    stats1.updateStats(Float.NaN);
    stats1.updateStats(3.0f);
    builder.add(stats1);

    // Page 2: all nulls
    FloatStatistics stats2 = (FloatStatistics) Statistics.createStats(type);
    stats2.incrementNumNulls(10);
    builder.add(stats2);

    // Page 3: no NaN
    FloatStatistics stats3 = (FloatStatistics) Statistics.createStats(type);
    stats3.updateStats(5.0f);
    stats3.updateStats(10.0f);
    builder.add(stats3);

    ColumnIndex columnIndex = builder.build();
    org.apache.parquet.format.ColumnIndex parquetColumnIndex =
        ParquetMetadataConverter.toParquetColumnIndex(type, columnIndex);
    assertThat(parquetColumnIndex).isNotNull();
    assertThat(parquetColumnIndex.getNan_counts())
        .as("nan_counts should be set")
        .isNotNull();
    assertThat(parquetColumnIndex.getNan_counts()).containsExactly(1L, 0L, 0L);

    ColumnIndex roundTrip = ParquetMetadataConverter.fromParquetColumnIndex(type, parquetColumnIndex);
    assertThat(roundTrip).isNotNull();
    assertThat(roundTrip.getNanCounts()).containsExactly(1L, 0L, 0L);
  }
}
