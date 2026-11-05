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
package org.apache.parquet.io;

import java.util.Arrays;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation.FiniteVectorLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

final class FiniteVectorValidator {
  private final ColumnReader column;
  private final String[] fieldPath;
  private final PrimitiveTypeName primitive;
  private final int maxDefinitionLevel;
  private final int maxRepetitionLevel;
  private final int length;
  private final boolean float16;
  private long count;

  FiniteVectorValidator(PrimitiveColumnIO columnIO, ColumnReader column, FiniteVectorLogicalTypeAnnotation vector) {
    this.column = column;
    this.fieldPath = columnIO.getFieldPath();
    this.primitive = columnIO.getType().asPrimitiveType().getPrimitiveTypeName();
    this.maxDefinitionLevel = columnIO.getDefinitionLevel();
    this.maxRepetitionLevel = columnIO.getRepetitionLevel();
    this.length = vector.getNumElements();
    this.float16 = columnIO.getType().getLogicalTypeAnnotation() instanceof Float16LogicalTypeAnnotation;
  }

  void validate(int definitionLevel) {
    int repetitionLevel = column.getCurrentRepetitionLevel();
    if (definitionLevel < 0
        || definitionLevel > maxDefinitionLevel
        || repetitionLevel < 0
        || repetitionLevel > maxRepetitionLevel) {
      throw invalidVector("invalid levels");
    }
    if (count == 0 && repetitionLevel == maxRepetitionLevel) {
      throw invalidVector("invalid continuation");
    }
    if (definitionLevel < maxDefinitionLevel) {
      if (count > 0) throw invalidVector("null within vector");
      if (definitionLevel == maxDefinitionLevel - 1) throw invalidVector("empty vector");
      return;
    }
    count++;
    if (count > length) {
      throw invalidVector("expected " + length + " elements, got " + count);
    }
    if (primitive == PrimitiveTypeName.FLOAT && !Float.isFinite(column.getFloat())
        || primitive == PrimitiveTypeName.DOUBLE && !Double.isFinite(column.getDouble())) {
      throw invalidVector("non-finite element");
    }
    if (float16) {
      Binary value = column.getBinary();
      if (value.length() != 2) {
        throw invalidVector("invalid FLOAT16 element length: " + value.length());
      }
      if ((value.get2BytesLittleEndian() & 0x7c00) == 0x7c00) {
        throw invalidVector("non-finite element");
      }
    }
  }

  void end(int nextRepetitionLevel) {
    if (nextRepetitionLevel < 0 || nextRepetitionLevel > maxRepetitionLevel) {
      throw invalidVector("invalid levels");
    }
    if (nextRepetitionLevel == maxRepetitionLevel && count == 0) {
      throw invalidVector("null within vector");
    }
    if (nextRepetitionLevel < maxRepetitionLevel && count > 0) {
      if (count != length) {
        throw invalidVector("expected " + length + " elements, got " + count);
      }
      count = 0;
    }
  }

  private ParquetDecodingException invalidVector(String reason) {
    return new ParquetDecodingException(
        "Invalid FINITE_VECTOR data for " + Arrays.toString(fieldPath) + ": " + reason);
  }
}
