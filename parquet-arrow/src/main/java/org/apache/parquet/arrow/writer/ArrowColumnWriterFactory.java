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
package org.apache.parquet.arrow.writer;

import org.apache.parquet.column.page.PageWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Factory that selects the optimal {@link ArrowColumnWriter} strategy for each column
 * based on the Parquet schema (type, nullability, encoding).
 *
 * <p>Strategy selection (best to worst):
 * <ol>
 *   <li>{@link ZeroCopyPlainWriter} — non-null, fixed-width, PLAIN encoding</li>
 *   <li>{@link NullablePlainWriter} — nullable, fixed-width, PLAIN encoding</li>
 *   <li>Fallback — per-value (not yet implemented, throws UnsupportedOperationException)</li>
 * </ol>
 */
final class ArrowColumnWriterFactory {

  private ArrowColumnWriterFactory() {}

  /**
   * Creates an ArrowColumnWriter for the column at the given index in the schema.
   *
   * @param schema the Parquet message type
   * @param columnIndex the column index
   * @param pageWriter the page writer for this column
   * @return the optimal column writer for this column's characteristics
   */
  static ArrowColumnWriter create(MessageType schema, int columnIndex, PageWriter pageWriter) {
    Type fieldType = schema.getType(columnIndex);

    if (!fieldType.isPrimitive()) {
      throw new UnsupportedOperationException(
          "Nested types are not yet supported by ArrowParquetWriter: " + fieldType);
    }

    PrimitiveType primitiveType = fieldType.asPrimitiveType();
    int maxDL = schema.getMaxDefinitionLevel(new String[]{fieldType.getName()});
    boolean isNullable = fieldType.getRepetition() == Type.Repetition.OPTIONAL;
    int typeWidth = getTypeWidth(primitiveType);

    // Boolean: special bit-packed handling
    if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
      return new BooleanPlainWriter(pageWriter, primitiveType, maxDL, isNullable);
    }

    // Variable-width (BINARY, string)
    if (primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY) {
      return new VarWidthPlainWriter(pageWriter, primitiveType, maxDL, isNullable);
    }

    // Fixed-width types
    if (typeWidth > 0) {
      if (!isNullable) {
        return new ZeroCopyPlainWriter(pageWriter, primitiveType, typeWidth, maxDL);
      } else {
        return new NullablePlainWriter(pageWriter, primitiveType, typeWidth, maxDL);
      }
    }

    throw new UnsupportedOperationException(
        "Unsupported type for ArrowParquetWriter: " + primitiveType);
  }

  /**
   * Returns the byte width for fixed-width primitive types, or -1 for variable-width.
   */
  private static int getTypeWidth(PrimitiveType type) {
    switch (type.getPrimitiveTypeName()) {
      case BOOLEAN:
        return -1; // Boolean is bit-packed, not fixed-width in the same sense
      case INT32:
      case FLOAT:
        return 4;
      case INT64:
      case DOUBLE:
        return 8;
      case INT96:
        return 12;
      case FIXED_LEN_BYTE_ARRAY:
        return type.getTypeLength();
      case BINARY:
      default:
        return -1; // Variable-width
    }
  }
}
