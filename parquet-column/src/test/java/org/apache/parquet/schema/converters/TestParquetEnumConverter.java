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

import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.ConvertedType;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestParquetEnumConverter
{
  @Test
  public void testEnumEquivalence() {
    ParquetSchemaConverter parquetMetadataConverter = new ParquetSchemaConverter();
    for (org.apache.parquet.schema.Type.Repetition repetition :
        org.apache.parquet.schema.Type.Repetition.values()) {
      assertEquals(
          repetition,
          ParquetEnumConverter.fromParquetRepetition(ParquetEnumConverter.toParquetRepetition(repetition)));
    }
    for (FieldRepetitionType repetition : FieldRepetitionType.values()) {
      assertEquals(
          repetition,
          ParquetEnumConverter.toParquetRepetition(ParquetEnumConverter.fromParquetRepetition(repetition)));
    }
    for (PrimitiveType.PrimitiveTypeName primitiveTypeName : PrimitiveType.PrimitiveTypeName.values()) {
      assertEquals(
          primitiveTypeName,
          ParquetEnumConverter.getPrimitive(ParquetEnumConverter.getType(primitiveTypeName)));
    }
    for (Type type : Type.values()) {
      assertEquals(type, ParquetEnumConverter.getType(ParquetEnumConverter.getPrimitive(type)));
    }
    for (OriginalType original : OriginalType.values()) {
      assertEquals(
          original,
          parquetMetadataConverter
              .getLogicalTypeAnnotation(
                  parquetMetadataConverter.convertToConvertedType(
                      LogicalTypeAnnotation.fromOriginalType(original, null)),
                  null)
              .toOriginalType());
    }
    for (ConvertedType converted : ConvertedType.values()) {
      assertEquals(
          converted,
          parquetMetadataConverter.convertToConvertedType(
              parquetMetadataConverter.getLogicalTypeAnnotation(converted, null)));
    }
  }

  @Test
  public void testEdgeInterpolationAlgorithmConversion() {
    // Test conversion from Parquet to Thrift enum
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm parquetAlgo = EdgeInterpolationAlgorithm.SPHERICAL;
    org.apache.parquet.format.EdgeInterpolationAlgorithm thriftAlgo =
        ParquetEnumConverter.fromParquetEdgeInterpolationAlgorithm(parquetAlgo);

    // convert the Thrift enum to the column schema enum
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm expected =
        org.apache.parquet.column.schema.EdgeInterpolationAlgorithm.SPHERICAL;
    org.apache.parquet.column.schema.EdgeInterpolationAlgorithm actual =
        ParquetEnumConverter.toParquetEdgeInterpolationAlgorithm(thriftAlgo);
    assertEquals(expected, actual);

    // Test with null
    assertNull(ParquetEnumConverter.fromParquetEdgeInterpolationAlgorithm(null));
    assertNull(ParquetEnumConverter.toParquetEdgeInterpolationAlgorithm(null));
  }
}
