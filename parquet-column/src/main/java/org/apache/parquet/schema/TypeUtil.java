/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.schema;

import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.FiniteVectorLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.Float16LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;

public class TypeUtil {
  public static boolean hasFiniteVector(Type type) {
    if (type.getLogicalTypeAnnotation() instanceof FiniteVectorLogicalTypeAnnotation) return true;
    if (!type.isPrimitive()) {
      for (Type child : type.asGroupType().getFields()) {
        if (hasFiniteVector(child)) return true;
      }
    }
    return false;
  }

  public static FiniteVectorLogicalTypeAnnotation getFiniteVectorType(GroupType schema, String[] path) {
    if (path.length < 3) {
      return null;
    }
    GroupType group = schema;
    for (int i = 0; i < path.length - 2; i++) {
      group = group.getType(path[i]).asGroupType();
    }
    LogicalTypeAnnotation logical = group.getLogicalTypeAnnotation();
    return logical instanceof FiniteVectorLogicalTypeAnnotation
        ? (FiniteVectorLogicalTypeAnnotation) logical
        : null;
  }

  static void checkFiniteVector(GroupType vector) {
    if (vector.isRepetition(Type.Repetition.REPEATED) || vector.getFieldCount() != 1) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    Type list = vector.getType(0);
    if (list.isPrimitive()
        || !list.getName().equals("list")
        || !list.isRepetition(Type.Repetition.REPEATED)
        || list.getLogicalTypeAnnotation() != null
        || list.asGroupType().getFieldCount() != 1) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    Type element = list.asGroupType().getType(0);
    if (!element.isPrimitive()
        || !element.getName().equals("element")
        || !element.isRepetition(Type.Repetition.REQUIRED)) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
    LogicalTypeAnnotation logical = element.getLogicalTypeAnnotation();
    boolean allowed = switch (element.asPrimitiveType().getPrimitiveTypeName()) {
      case BOOLEAN, FLOAT, DOUBLE -> logical == null;
      case INT32, INT64 ->
        logical == null
            || logical instanceof IntLogicalTypeAnnotation
            || logical instanceof DecimalLogicalTypeAnnotation;
      case FIXED_LEN_BYTE_ARRAY ->
        logical instanceof Float16LogicalTypeAnnotation || logical instanceof DecimalLogicalTypeAnnotation;
      default -> false;
    };
    if (!allowed) {
      throw new InvalidSchemaException("Invalid FINITE_VECTOR schema: " + vector);
    }
  }

  public static void checkValidWriteSchema(GroupType schema) {
    schema.accept(new TypeVisitor() {
      @Override
      public void visit(GroupType groupType) {
        if (groupType.getLogicalTypeAnnotation() instanceof FiniteVectorLogicalTypeAnnotation) {
          checkFiniteVector(groupType);
        }
        if (groupType.getFieldCount() <= 0) {
          throw new InvalidSchemaException("Cannot write a schema with an empty group: " + groupType);
        }
        for (Type type : groupType.getFields()) {
          type.accept(this);
        }
      }

      @Override
      public void visit(MessageType messageType) {
        visit((GroupType) messageType);
      }

      @Override
      public void visit(PrimitiveType primitiveType) {}
    });
  }
}
