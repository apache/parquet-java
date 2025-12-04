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

import org.apache.parquet.format.EdgeInterpolationAlgorithm;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.TimeUnit;
import org.apache.parquet.format.Type;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

/**
 * This class contains logic to convert between thrift (parquet-format) generated enums and enums in parquet-column.
 */
public class ParquetEnumConverter {
  public static PrimitiveType.PrimitiveTypeName getPrimitive(Type type) {
    switch (type) {
      case BYTE_ARRAY: // TODO: rename BINARY and remove this switch
        return PrimitiveType.PrimitiveTypeName.BINARY;
      case INT64:
        return PrimitiveType.PrimitiveTypeName.INT64;
      case INT32:
        return PrimitiveType.PrimitiveTypeName.INT32;
      case BOOLEAN:
        return PrimitiveType.PrimitiveTypeName.BOOLEAN;
      case FLOAT:
        return PrimitiveType.PrimitiveTypeName.FLOAT;
      case DOUBLE:
        return PrimitiveType.PrimitiveTypeName.DOUBLE;
      case INT96:
        return PrimitiveType.PrimitiveTypeName.INT96;
      case FIXED_LEN_BYTE_ARRAY:
        return PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new RuntimeException("Unknown type " + type);
    }
  }

  public static Type getType(PrimitiveType.PrimitiveTypeName type) {
    switch (type) {
      case INT64:
        return Type.INT64;
      case INT32:
        return Type.INT32;
      case BOOLEAN:
        return Type.BOOLEAN;
      case BINARY:
        return Type.BYTE_ARRAY;
      case FLOAT:
        return Type.FLOAT;
      case DOUBLE:
        return Type.DOUBLE;
      case INT96:
        return Type.INT96;
      case FIXED_LEN_BYTE_ARRAY:
        return Type.FIXED_LEN_BYTE_ARRAY;
      default:
        throw new RuntimeException("Unknown primitive type " + type);
    }
  }

  /** Convert Parquet Algorithm enum to Thrift Algorithm enum */
  public static EdgeInterpolationAlgorithm fromParquetEdgeInterpolationAlgorithm(
      org.apache.parquet.column.schema.EdgeInterpolationAlgorithm parquetAlgo) {
    if (parquetAlgo == null) {
      return null;
    }
    EdgeInterpolationAlgorithm thriftAlgo = EdgeInterpolationAlgorithm.findByValue(parquetAlgo.getValue());
    if (thriftAlgo == null) {
      throw new IllegalArgumentException("Unrecognized Parquet EdgeInterpolationAlgorithm: " + parquetAlgo);
    }
    return thriftAlgo;
  }

  /** Convert Thrift Algorithm enum to Parquet Algorithm enum */
  public static org.apache.parquet.column.schema.EdgeInterpolationAlgorithm toParquetEdgeInterpolationAlgorithm(
      EdgeInterpolationAlgorithm thriftAlgo) {
    if (thriftAlgo == null) {
      return null;
    }
    return org.apache.parquet.column.schema.EdgeInterpolationAlgorithm.findByValue(thriftAlgo.getValue());
  }

  // Visible for testing
  static FieldRepetitionType toParquetRepetition(org.apache.parquet.schema.Type.Repetition repetition) {
    return FieldRepetitionType.valueOf(repetition.name());
  }

  // Visible for testing
  static org.apache.parquet.schema.Type.Repetition fromParquetRepetition(FieldRepetitionType repetition) {
    return org.apache.parquet.schema.Type.Repetition.valueOf(repetition.name());
  }

  static LogicalTypeAnnotation.TimeUnit convertTimeUnit(TimeUnit unit) {
    switch (unit.getSetField()) {
      case MICROS:
        return LogicalTypeAnnotation.TimeUnit.MICROS;
      case MILLIS:
        return LogicalTypeAnnotation.TimeUnit.MILLIS;
      case NANOS:
        return LogicalTypeAnnotation.TimeUnit.NANOS;
      default:
        throw new RuntimeException("Unknown time unit " + unit);
    }
  }
}
